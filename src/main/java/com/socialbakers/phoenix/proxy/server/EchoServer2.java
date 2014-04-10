/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.socialbakers.phoenix.proxy.server;

import com.salesforce.phoenix.jdbc.PhoenixDriver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos;
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
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EchoServer2 {

    private InetAddress addr;
    private int port;
    private Selector selector;
    private Map<SocketChannel, List<byte[]>> outgoingData;
    private Map<SocketChannel, IncomingData> incomingData;

    private Connection conn = null;
    private int connectionCounter = 0;
    private String zooKeeper = "hadoops-master.us-w2.aws.ccl";

    private BlockingQueue<Runnable> linkedBlockingDeque = new ArrayBlockingQueue<Runnable>(20000);
    private ExecutorService executor = new ThreadPoolExecutor(256, 256, 30, TimeUnit.MINUTES, linkedBlockingDeque);

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
            
    public EchoServer2(InetAddress addr, int port, String zooKeeper) throws IOException, SQLException {
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

        log("Echo server ready. Ctrl-C to stop.");

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

                if (! key.isValid()) {
                    continue;
                }

                if (key.isAcceptable()) {
                    this.accept(key);
                }
                else if (key.isReadable()) {
                    this.read(key);
                }
                else if (key.isWritable()) {
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
        }
        catch (IOException e) {
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
                
        public PhoenixProxyProtos.QueryResponse sendQuery(String query, int callId) throws Exception {
            
            long start = System.currentTimeMillis();
            
            PhoenixProxyProtos.QueryResponse.Builder responseBuilder
                    = PhoenixProxyProtos.QueryResponse.newBuilder()
                    .setCallId(callId);

            try {
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
                    PhoenixProxyProtos.ColumnMapping.Type columnType
                            = getColumnType(meta.getColumnType(i));

                    PhoenixProxyProtos.ColumnMapping column
                            = PhoenixProxyProtos.ColumnMapping.newBuilder()
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
                    
                    PhoenixProxyProtos.Row.Builder rowBuilder
                            = PhoenixProxyProtos.Row.newBuilder();
                    
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
                
            } catch (Exception e) {
                PhoenixProxyProtos.QueryException exception 
                        = PhoenixProxyProtos.QueryException.newBuilder()
                                .setMessage(e.getMessage())
                                .build();
                responseBuilder.setException(exception);
            }

            PhoenixProxyProtos.QueryResponse response = responseBuilder.build();
            
            long queryAndBuildDuration = System.currentTimeMillis() - start;
            log("Whole query duration: " + queryAndBuildDuration + "ms");
            
            return response;
        }
        
        private ByteString getValue(ResultSet rs, ResultSetMetaData meta, 
                int column) throws SQLException {

            byte[] bytes = null;
            if (meta.getColumnType(column) == Types.DATE) {
                Date d = (Date)rs.getObject(column);
                if (d != null) {
                    long time = d.getTime();
                    log("Date(long) value:" + time);
                    ByteBuffer wrapper = ByteBuffer.allocate(8);
                    wrapper.putLong(time);
                    bytes = wrapper.array();
                }
            } else {
                bytes = rs.getBytes(column);
            }
            
            if (bytes == null || rs.wasNull()) {
                return ByteString.EMPTY;
            }
            
            return ByteString.copyFrom(bytes);
        }
        
        private PhoenixProxyProtos.ColumnMapping.Type getColumnType(int type) {
            PhoenixProxyProtos.ColumnMapping.Type columnType;
            if (type == Types.INTEGER) {
                columnType = PhoenixProxyProtos.ColumnMapping.Type.INTEGER;
            } else if (type == Types.DOUBLE) {
                columnType = PhoenixProxyProtos.ColumnMapping.Type.DOUBLE;
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

                log("Query: " + query);  
                
                // JDBC read data
                PhoenixProxyProtos.QueryResponse response = sendQuery(query, callId);
                
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
            throw new IllegalArgumentException("You must pass 2 parameters: <port> <zooKeeper>");
        }
        
        Integer port = Integer.valueOf(args[0]);
        String zooKeeper = args[1];
        new EchoServer2(null, port, zooKeeper);
    }
}