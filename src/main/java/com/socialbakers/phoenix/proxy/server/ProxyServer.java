package com.socialbakers.phoenix.proxy.server;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.StringUtils;

public class ProxyServer {

    private static final Logger logger = Logger.getLogger(QueryProcessor.class.getName());
    
    private InetAddress addr;
    private int port;
    private Selector selector;
    private Map<SocketChannel, List<byte[]>> outgoingData;
    private Map<SocketChannel, IncomingData> incomingData;
    private QueryProcessor queryProcessor;
    
    public ProxyServer(InetAddress addr, int port, String zooKeeper) throws SQLException {
	
        this.addr = addr;
	this.port = port;
	
	queryProcessor = new QueryProcessor(zooKeeper);
	outgoingData = new HashMap<SocketChannel, List<byte[]>>();
	incomingData = new HashMap<SocketChannel, IncomingData>();
    }

    public void startServer() throws IOException {
        
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
	    log(e);
	}

	if (numRead == -1) {
	    this.outgoingData.remove(channel);
            this.incomingData.remove(channel);
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
	    RequestProcessor processor = new RequestProcessor(key, message, queryProcessor);
	    processor.start();
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

    
    private static void log(String msg) {
        logger.log(Level.SEVERE, msg);
    }

    private static void log(Throwable t) {
        log(null, t);
    }
    
    private static void log(String msg, Throwable t) {
        if (StringUtils.isBlank(msg) && t != null) {
            msg = t.getMessage();
        }
        logger.log(Level.SEVERE, msg, t);
    }

    public static void main(String[] args) throws Exception {

	if (args.length < 2) {
	    System.err.println("You must pass 2 parameters: <port> <zooKeeper>");
	    System.exit(1);
	}

	Integer port = Integer.valueOf(args[0]);
	String zooKeeper = args[1];
        ProxyServer proxyServer = new ProxyServer(null, port, zooKeeper);
        proxyServer.startServer();
    }
}