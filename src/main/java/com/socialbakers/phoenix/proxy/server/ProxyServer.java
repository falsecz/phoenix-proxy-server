package com.socialbakers.phoenix.proxy.server;

import com.salesforce.phoenix.jdbc.PhoenixDriver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import com.google.protobuf.ByteString;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProxyServer {

    private InetAddress addr;
    private int port;
    private Selector selector;
    private Map<SocketChannel, List<byte[]>> outgoingData;
    private Map<SocketChannel, IncomingData> incomingData;
    private Connection conn = null;
    private int connectionCounter = 0;
    private String zooKeeper;  //= "hadoops-master.us-w2.aws.ccl";
    private BlockingQueue<Runnable> linkedBlockingDeque;
//    private ExecutorService executor;

    private Connection getConnection() throws SQLException {
	if (conn != null) {
	    connectionCounter++;

	    //create new connection
	    if (connectionCounter == 500) {
		new Thread(new Runnable() {
		    @Override
		    public void run() {
			try {
			    System.out.println("creating new connection");
			    Connection c = DriverManager.getConnection("jdbc:phoenix:" + zooKeeper);
			    System.out.println("new connection created");

			    conn = c;
			    connectionCounter = 0;
			} catch (SQLException ex) {
			    Logger.getLogger(RequestProcessor.class.getName()).log(Level.SEVERE, null, ex);
			}
		    }
		}).start();
	    }

	    return conn;
	}

	DriverManager.registerDriver(new PhoenixDriver());
	conn = DriverManager.getConnection("jdbc:phoenix:" + zooKeeper);
	return conn;
    }

    private class IncomingData {

	int len;
	int read;
	byte[] data;

	IncomingData(int len) {
	    this.len = len;
	    this.read = 0;
	    this.data = new byte[len];
	}

	void read(byte[] buf, int size) {
	    System.arraycopy(buf, 0, data, read, size);
	    read += size;
	}

	int left() {
	    return len - read;
	}

	boolean isComplete() {
	    return left() == 0;
	}

	PhoenixProxyProtos.QueryRequest toQueryRequest() throws IOException {
	    return PhoenixProxyProtos.QueryRequest.newBuilder()
		    .mergeFrom(data)
		    .build();
	}
    }

    public ProxyServer(InetAddress addr, int port, String zooKeeper) throws IOException, SQLException {
	this.linkedBlockingDeque = new ArrayBlockingQueue<Runnable>(20000);
	//this.executor = new ThreadPoolExecutor(256, 256, 30, TimeUnit.MINUTES, linkedBlockingDeque);
	this.addr = addr;
	this.port = port;
	this.zooKeeper = zooKeeper;
	outgoingData = new HashMap<SocketChannel, List<byte[]>>();
	incomingData = new HashMap<SocketChannel, IncomingData>();
	getConnection();
	startServer();
    }

    private void startServer() throws IOException {
	// create selector and channel
	this.selector = Selector.open();
	ServerSocketChannel serverChannel = ServerSocketChannel.open();
	serverChannel.configureBlocking(false);

	// bind to port
	InetSocketAddress listenAddr = new InetSocketAddress(this.addr, this.port);
	serverChannel.socket().bind(listenAddr);
	serverChannel.register(this.selector, SelectionKey.OP_ACCEPT);

	log("Phoenix proxy listening on " + this.addr + ":" + this.port);

	// processing
	while (true) {
	    // wait for events
	    this.selector.select();

	    // wakeup to work on selected keys
	    Iterator keys = this.selector.selectedKeys().iterator();
	    while (keys.hasNext()) {
		SelectionKey key = (SelectionKey) keys.next();

		// this is necessary to prevent the same key from coming up 
		// again the next time around.
		keys.remove();

		if (!key.isValid()) {
		    continue;
		}

		if (key.isAcceptable()) {
		    this.accept(key);
		} else if (key.isReadable()) {
		    this.read(key);
		} else if (key.isWritable()) {
		    this.write(key);
		}
	    }
	}
    }

    private void accept(SelectionKey key) throws IOException {
	ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
	SocketChannel channel = serverChannel.accept();
	channel.configureBlocking(false);

	// write welcome message
//        channel.write(ByteBuffer.wrap("Welcome, this is the echo server\r\n".getBytes("US-ASCII")));

	Socket socket = channel.socket();
	SocketAddress remoteAddr = socket.getRemoteSocketAddress();
	log("Connected to: " + remoteAddr);

	// register channel with selector for further IO
	outgoingData.put(channel, new ArrayList<byte[]>());
	channel.register(this.selector, SelectionKey.OP_READ);
    }

    private void read(SelectionKey key) throws IOException {

	SocketChannel channel = (SocketChannel) key.channel();
	IncomingData message = incomingData.get(channel);
	// new incoming message or continue incomplete message
	int bytesToRead = message == null ? 4 : message.left();

	ByteBuffer buffer = ByteBuffer.allocate(bytesToRead);
	int numRead = -1;
	try {
	    numRead = channel.read(buffer);
	} catch (IOException e) {
	    e.printStackTrace();
	}

	if (numRead == -1) {
	    this.outgoingData.remove(channel);
	    Socket socket = channel.socket();
	    SocketAddress remoteAddr = socket.getRemoteSocketAddress();
	    log("Connection closed by client: " + remoteAddr);
	    channel.close();
	    key.cancel();
	    return;
	}

	if (message == null) {
	    // new request
	    byte[] data = new byte[numRead];
	    System.arraycopy(buffer.array(), 0, data, 0, numRead);
	    ByteBuffer wrapped = ByteBuffer.wrap(data);
	    int len = wrapped.getInt();
	    log("Got len of new message: " + len);
	    message = new IncomingData(len);
	    incomingData.put(channel, message);
	} else {
	    // continue reading
	    message.read(buffer.array(), numRead);
	}

	if (message.isComplete()) {
	    incomingData.remove(channel);
	    RequestProcessor processor = new RequestProcessor(key, message);
	    processor.start();
	}
    }

    private class RequestProcessor extends Thread {

	private SelectionKey key;
	private IncomingData message;

	private RequestProcessor(SelectionKey key, IncomingData message) {
	    this.key = key;
	    this.message = message;
	}

	public PhoenixProxyProtos.QueryResponse sendQuery(String query, int callId, PhoenixProxyProtos.QueryRequest.Type type) throws Exception {

	    long start = System.currentTimeMillis();

	    PhoenixProxyProtos.QueryResponse.Builder responseBuilder = PhoenixProxyProtos.QueryResponse.newBuilder()
		    .setCallId(callId);

	    try {
		if (type == PhoenixProxyProtos.QueryRequest.Type.UPDATE) {
		    // getConnection().createStatement().execute(query);
		    Connection con = getConnection();
		    PreparedStatement upsertStmt = con.prepareStatement(query);
		    int rowsInserted = upsertStmt.executeUpdate();
		    //		    int c = getConnection().createStatement().executeUpdate(query);
		    log("Updated lines count:" + rowsInserted);
		    con.commit();
			    
		} else {
		    // select data
		    ResultSet rs = getConnection().createStatement().executeQuery(query);

		    long queryExecuted = System.currentTimeMillis();
		    long queryDuration = queryExecuted - start;
		    log("Query execution duration: " + queryDuration + "ms");

		    ResultSetMetaData meta = rs.getMetaData();
		    int columnCount = meta.getColumnCount();

		    // column metadata
		    for (int i = 1; i <= columnCount; i++) {
			String columnName = meta.getColumnName(i);
			PhoenixProxyProtos.ColumnMapping.Type columnType = getColumnType(meta.getColumnType(i));

			PhoenixProxyProtos.ColumnMapping column = PhoenixProxyProtos.ColumnMapping.newBuilder()
				.setName(columnName)
				.setType(columnType)
				.build();
			responseBuilder.addMapping(column);
		    }

		    long metadataRead = System.currentTimeMillis();
		    long metaDuration = metadataRead - queryExecuted;
		    log("Metadata reading duration: " + metaDuration + "ms");

		    long resultsetDuration = 0;
		    // data
		    while (rs.next()) {

			long resultsetRowStart = System.currentTimeMillis();

			PhoenixProxyProtos.Row.Builder rowBuilder = PhoenixProxyProtos.Row.newBuilder();

			for (int i = 1; i <= columnCount; i++) {
			    ByteString value = getValue(rs, meta, i);
			    rowBuilder.addBytes(value);
			}

			PhoenixProxyProtos.Row row = rowBuilder.build();
			responseBuilder.addRows(row);

			long resultsetRowDuration = System.currentTimeMillis() - resultsetRowStart;
			resultsetDuration += resultsetRowDuration;
		    }
		    log("Resultset mapping duration: " + resultsetDuration + "ms");
		}

	    } catch (Exception e) {
		e.printStackTrace();
		PhoenixProxyProtos.QueryException exception = PhoenixProxyProtos.QueryException.newBuilder()
			.setMessage("mrdka")
			.build();
		responseBuilder.setException(exception);
	    }

	    PhoenixProxyProtos.QueryResponse response = responseBuilder.build();

	    long queryAndBuildDuration = System.currentTimeMillis() - start;
	    log("Whole query duration: " + queryAndBuildDuration + "ms");

	    return response;
	}

	private ByteString getValue(ResultSet rs, ResultSetMetaData meta,
		int column) throws SQLException, Exception {

	    Object o = rs.getObject(column);
	    if (o == null || rs.wasNull()) {
		return ByteString.EMPTY;
	    }

	    ByteBuffer b = null;
	    int type = meta.getColumnType(column);

	    if (type == Types.DATE) {
		Date d = (Date) rs.getObject(column);
		b = ByteBuffer.allocate(Long.SIZE / 8)
			.putLong(d.getTime());
	    } else if (type == Types.TINYINT) {
		b = ByteBuffer.allocate(1).put((Byte) o);
	    } else if (type == Types.BOOLEAN) {
		byte by = 0;
		if ((Boolean) o == true) {
		    by = 1;
		}
		b = ByteBuffer.allocate(1).put(by);

	    } else if (type == Types.INTEGER) {
		b = ByteBuffer.allocate(Integer.SIZE / 8).putInt((Integer) o);
	    } else if (type == Types.BIGINT) {
		b = ByteBuffer.allocate(Long.SIZE / 8).putLong((Long) o);
	    } else if (type == Types.VARCHAR) {
		String s = (String) o;
		b = ByteBuffer.wrap(ByteString.copyFromUtf8(s).toByteArray());
//		b = ByteBuffer.allocate(s.length()).pu putLong((Long) o);
	    } else if (meta.getColumnType(column) == Types.INTEGER) {
		Integer v = (Integer) rs.getObject(column);
		b = ByteBuffer.allocate(Integer.SIZE / 8).putInt(v);


	    } else {
		throw new Exception("Missing mapping for type " + type);
		// TODO tuto musi pryc, vsechno to musi bejt premapovany pres getObject
		// http://forcedotcom.github.io/phoenix/datatypes.html#tinyint_type
		// protoze interne je to namapovany jinak
	    }
	    if(b == null) {
		throw new Exception("Type " + type + " was not mapped");
		
	    }

	    return ByteString.copyFrom(b.array());
	}

	private PhoenixProxyProtos.ColumnMapping.Type getColumnType(int type) {
	    PhoenixProxyProtos.ColumnMapping.Type columnType;
	    if (type == Types.INTEGER) {
		columnType = PhoenixProxyProtos.ColumnMapping.Type.INTEGER;
	    } else if (type == Types.DOUBLE) {
		columnType = PhoenixProxyProtos.ColumnMapping.Type.DOUBLE;
	    } else if (type == Types.TINYINT) {
		columnType = PhoenixProxyProtos.ColumnMapping.Type.TINYINT;
	    } else if (type == Types.FLOAT) {
		columnType = PhoenixProxyProtos.ColumnMapping.Type.FLOAT;
	    } else if (type == Types.BIGINT) {
		columnType = PhoenixProxyProtos.ColumnMapping.Type.BIGINT;
	    } else if (type == Types.BINARY) {
		columnType = PhoenixProxyProtos.ColumnMapping.Type.BINARY;
	    } else if (type == Types.BOOLEAN) {
		columnType = PhoenixProxyProtos.ColumnMapping.Type.BOOLEAN;
	    } else if (type == Types.TIMESTAMP) {
		columnType = PhoenixProxyProtos.ColumnMapping.Type.TIMESTAMP;
	    } else if (type == Types.DATE) {
		columnType = PhoenixProxyProtos.ColumnMapping.Type.DATE;
	    } else {
//		System.out.println("typ pro "+ type + " " + type);
		columnType = PhoenixProxyProtos.ColumnMapping.Type.VARCHAR;
	    }
	    return columnType;
	}

	@Override
	public void run() {

	    try {

//                Thread.sleep(2000);

		PhoenixProxyProtos.QueryRequest queryRequest = message.toQueryRequest();

		int callId = queryRequest.getCallId();
		String query = queryRequest.getQuery();
		PhoenixProxyProtos.QueryRequest.Type type = queryRequest.getType();
		log("Query: " + query);

		// JDBC read data
		PhoenixProxyProtos.QueryResponse response = sendQuery(query, callId, type);

		// data to bytes
		byte[] data = response.toByteArray();

		int len = data.length;
		ByteBuffer lenBuf = ByteBuffer.allocate(4);
		lenBuf.putInt(len);
		byte[] lenBytes = lenBuf.array();

		// response
		log("response len: " + data.length);
		SocketChannel channel = (SocketChannel) key.channel();
		byte[] data2 = new byte[data.length + lenBytes.length];
		System.arraycopy(lenBytes, 0, data2, 0, lenBytes.length);
		System.arraycopy(data, 0, data2, lenBytes.length, data.length);
		channel.write(ByteBuffer.wrap(data2));
//                doEcho(key, lenBytes);
//                doEcho(key, data);
	    } catch (IOException e) {
		e.printStackTrace();
//                Logger.getLogger(EchoServer.class.getName()).log(Level.SEVERE, null, e);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//                Logger.getLogger(EchoServer2.class.getName()).log(Level.SEVERE, null, e);
	    } catch (Exception e) {
		e.printStackTrace();
//                Logger.getLogger(EchoServer2.class.getName()).log(Level.SEVERE, null, ex);
	    }
	}
    }

    private void write(SelectionKey key) throws IOException {
	SocketChannel channel = (SocketChannel) key.channel();
	List<byte[]> pendingData = this.outgoingData.get(channel);
	Iterator<byte[]> items = pendingData.iterator();
	while (items.hasNext()) {
	    byte[] item = items.next();
	    items.remove();
	    channel.write(ByteBuffer.wrap(item));
	}
	key.interestOps(SelectionKey.OP_READ);
    }

    private void doEcho(SelectionKey key, byte[] data) {
	SocketChannel channel = (SocketChannel) key.channel();
	List<byte[]> pendingData = this.outgoingData.get(channel);
	pendingData.add(data);
	key.interestOps(SelectionKey.OP_WRITE);
    }

    private static void log(String s) {
	System.out.println(s);
    }

    public static void main(String[] args) throws Exception {

	if (args.length < 2) {
	    System.err.println("You must pass 2 parameters: <port> <zooKeeper>");
	    System.exit(1);
	}

	Integer port = Integer.valueOf(args[0]);
	String zooKeeper = args[1];
	new ProxyServer(null, port, zooKeeper);
    }
}