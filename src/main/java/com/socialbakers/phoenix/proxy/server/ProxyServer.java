package com.socialbakers.phoenix.proxy.server;

import static com.socialbakers.phoenix.proxy.server.Logger.*;
import static com.socialbakers.phoenix.proxy.server.Logger.error;

import com.socialbakers.phoenix.proxy.PhoenixProxyProtos;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.sql.SQLException;
import java.util.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import javax.management.MBeanServer;
import javax.management.ObjectName;

public class ProxyServer implements ProxyServerMBean {

    private InetAddress addr;
    private int port;
    private Selector selector;
    private Map<SocketChannel, List<byte[]>> outgoingData;
    private Map<SocketChannel, IncomingData> incomingData;
    private QueryProcessor queryProcessor;
    private RequestPool requestPool;
    private long connectionCounter;
    private long requestCounter;
    private long rejectionCounter;
    private int maxRequestLen = 10 * 1024 * 1024; // 10MB
    
    private static final String rejectionMsgFormat
            = "Request rejected becuse of queue overflow. corePoolSize:%d maxPoolSize:%d active:%d inQueue:%d";

    private RequestProcessor.SocketWriter writer = new RequestProcessor.SocketWriter() {
        @Override
        public void write(SelectionKey key, byte[] data) {
            
            synchronized (key) {

                SocketChannel channel = (SocketChannel) key.channel();
                
                ByteBuffer wrap = ByteBuffer.allocate(data.length + 4);
                wrap.putInt(data.length);
                wrap.put(data);
                wrap.flip();

                while (wrap.hasRemaining()) {
                    try {
                        channel.write(wrap);
                    } catch (IOException ex) {
                        error("closing connection cause of exception.", ex);
                        closeChannel(key);
                        return;
                    }
                }
            }
        }
    };
    
    private RejectedExecutionHandler rejectionHandler = new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            RequestProcessor request = (RequestProcessor) r;
            rejectRequest(request);
        }
    };
    
    public ProxyServer(InetAddress addr, int port, String zooKeeper,
            int corePoolSize, int maximumPoolSize, long keepAliveTimeMs, int queueSize) throws SQLException {
	
        this.addr = addr;
	this.port = port;
	this.requestPool = new RequestPool(corePoolSize, maximumPoolSize, keepAliveTimeMs, queueSize, 
                rejectionHandler);
        
	queryProcessor = new QueryProcessor(zooKeeper);
	outgoingData = new ConcurrentHashMap<SocketChannel, List<byte[]>>();
	incomingData = new ConcurrentHashMap<SocketChannel, IncomingData>();
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
        
	debug("Phoenix proxy listening on " + listenAddr.getAddress().getHostAddress() + ":" + this.port);

	// processing
	while (true) {

            // wait for events
            int readyChannels = selector.select();
            if (readyChannels == 0) continue;
            
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
//		} else if (key.isWritable()) {
//		    this.write(key);
		}
	    }
	}
    }

    private synchronized void accept(SelectionKey key) throws IOException {
	ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
	SocketChannel channel = serverChannel.accept();
	channel.configureBlocking(false);

	Socket socket = channel.socket();
	SocketAddress remoteAddr = socket.getRemoteSocketAddress();
	debug("Connected to: " + remoteAddr);

	// register channel with selector for further IO
	outgoingData.put(channel, new ArrayList<byte[]>());
        incomingData.put(channel, new IncomingData());
	channel.register(this.selector, SelectionKey.OP_READ);
        connectionCounter++;
    }

    private synchronized void read(SelectionKey key) throws IOException {

	SocketChannel channel = (SocketChannel) key.channel();
	IncomingData message = incomingData.get(channel);
        
        if (message == null) {
            SocketAddress adress = channel.socket().getRemoteSocketAddress();
            error(new IllegalStateException("No message for channel " + adress));
            closeChannel(key);
            return;
        }
	int bytesToRead = message.left();

	ByteBuffer buffer = ByteBuffer.allocate(bytesToRead);
	int numRead = -1;
	try {
	    numRead = channel.read(buffer);
	} catch (IOException e) {
	    error(e);
	}

	if (numRead == -1) {
            closeChannel(key);
	    return;
	}

        try {
            // continue reading
            message.read(buffer.array(), numRead);

            if (message.isComplete()) {
                // prepare new message for next read
                incomingData.put(channel, new IncomingData());
                PhoenixProxyProtos.QueryRequest queryRequest = message.toQueryRequest();
                RequestProcessor processor = new RequestProcessor(key, queryRequest, queryProcessor, writer);
                requestPool.execute(processor);
                requestCounter++;
            }
        } catch (Exception e) {
            error(e);
            closeChannel(key);
        }
    }
    
    private synchronized void closeChannel(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        this.outgoingData.remove(channel);
        this.incomingData.remove(channel);
        Socket socket = channel.socket();
        SocketAddress remoteAddr = socket.getRemoteSocketAddress();
        debug("Connection closed: " + remoteAddr);
        try {
            channel.close();
        } catch (IOException ex1) {
            error(ex1);
        }
        key.cancel();
    }
    
    private synchronized void rejectRequest(RequestProcessor request) {
        
        rejectionCounter++;
        requestCounter--;   // request not actualy processed but was counted
        
        int corePoolSize = requestPool.getCorePoolSize();
        int maximumPoolSize = requestPool.getMaximumPoolSize();
        int activeCount = requestPool.getActiveCount();
        int inQueue = requestPool.getCmdCountInQueue();

        String msg = String.format(rejectionMsgFormat, corePoolSize, maximumPoolSize, activeCount, inQueue);
        PhoenixProxyProtos.QueryResponse response = request.createExceptionResponse(msg);
        byte[] bytes = response.toByteArray();

        writer.write(request.getKey(), bytes);
        debug(msg);
    }
    
//    
//    private void write(SelectionKey key) throws IOException {
//        
//	SocketChannel channel = (SocketChannel) key.channel();
//	List<byte[]> pendingData = this.outgoingData.get(channel);
//	Iterator<byte[]> items = pendingData.iterator();
//        
//        while (items.hasNext()) {
//            byte[] item = items.next();
//            items.remove();
//            ByteBuffer wrap = ByteBuffer.wrap(item);
//            while (wrap.hasRemaining()) {
//                channel.write(wrap);
//            }
//        }
//
//	key.interestOps(SelectionKey.OP_READ);
//    }

//    private void doEcho(SelectionKey key, byte[] data) {
//	SocketChannel channel = (SocketChannel) key.channel();
//	List<byte[]> pendingData = this.outgoingData.get(channel);
//	pendingData.add(data);
//	key.interestOps(SelectionKey.OP_WRITE);
//    }
    
    @Override
    public int getActiveConnections() {
        return incomingData.size();
    }

    @Override
    public int getActiveRequests() {
        return requestPool.getActiveCount();
    }

    @Override
    public int getRequestsInQueue() {
        return requestPool.getCmdCountInQueue();
    }

    @Override
    public int getPoolSize() {
        return requestPool.getPoolSize();
    }

    @Override
    public int getCorePoolSize() {
        return requestPool.getCorePoolSize();
    }

    @Override
    public int getMaxPoolSize() {
        return requestPool.getMaximumPoolSize();
    }

    @Override
    public int getQueueSize() {
        return requestPool.getQueueSize();
    }

    @Override
    public long getConnectionCount() {
        return connectionCounter;
    }

    @Override
    public long getRequestCount() {
        return requestCounter;
    }

    @Override
    public long getErrorCount() {
        return getErrCount();
    }

    @Override
    public long getRejectionCount() {
        return rejectionCounter;
    }
    
    @Override
    public void setMaxRequestLen(int maxRequestLen) {
        this.maxRequestLen = maxRequestLen;
        IncomingData.maxRequestLen = maxRequestLen;
    }

    @Override
    public int getMaxRequestLen() {
        return maxRequestLen;
    }
    
    @Override
    public synchronized void resetCounters() {
        resetErrCounter();
        connectionCounter = 0;
        requestCounter = 0;
        rejectionCounter = 0;
    }

    
    // ----------------------------------------------- STATIC MAIN -------------------------------------------------- //

    // Environment variables
    private static final String ZE = "PHOENIX_ZK";      // zooKeeper jdbc url
    private static final String PE = "PORT";            // port
    private static final String CE = "CORE_POOL_SIZE";  // core pool size
    private static final String ME = "MAX_POOL_SIZE";   // max pool size
    private static final String QE = "QUEUE_SIZE";      // queue size
    private static final String KE = "KEEP_ALIVE_TIME"; // keep alive time in milliseconds
    private static final String RE = "MAX_REQUEST_LEN"; // maximum length of request message in bytes
    
    // Program parameters (Overides Env vars)
    private static final String C = "-c"; // core pool size
    private static final String M = "-m"; // max pool size
    private static final String Q = "-q"; // queue size
    private static final String K = "-k"; // keep alive time in milliseconds
    private static final String R = "-r"; // maximum length of request message in bytes
    

    
    public static void main(String[] args) throws Exception {

	String zooKeeper = null;
        Integer port = null;
        int corePoolSize = 128;
        int maxPoolSize = 128;
        int queueSize = 20000;
        int keepAliveInMillis = 20000;
        int maxRequestLen = 10*1024*1024; // 10 MB
        
        if (System.getenv(ZE) != null) {            
            zooKeeper = System.getenv(ZE);
        } else if (System.getenv(PE) != null) {
             port = Integer.valueOf(System.getenv(PE));
        } else if (System.getenv(CE) != null) {
            corePoolSize = Integer.valueOf(System.getenv(CE));
        } else if (System.getenv(ME) != null) {
            maxPoolSize = Integer.valueOf(System.getenv(ME));
        } else if (System.getenv(QE) != null) {
            queueSize = Integer.valueOf(System.getenv(QE));
        } else if (System.getenv(KE) != null) {
            keepAliveInMillis = Integer.valueOf(System.getenv(KE));
        }
        
        if (args.length > 0 && !args[0].startsWith("-")) {
            port = Integer.valueOf(args[0]);
        }
        if (args.length > 1 && !args[1].startsWith("-")) {
            zooKeeper = args[1];
        }
        
        for (String arg : args) {
            if (arg.startsWith(C)) {
                corePoolSize = Integer.valueOf(arg.replaceFirst(C, ""));
            } else if (arg.startsWith(M)) {
                maxPoolSize = Integer.valueOf(arg.replaceFirst(M, ""));
            } else if (arg.startsWith(Q)) {
                queueSize = Integer.valueOf(arg.replaceFirst(Q, ""));
            } else if (arg.startsWith(K)) {
                keepAliveInMillis = Integer.valueOf(arg.replaceFirst(K, ""));
            }
        }
        
        if (port == null || zooKeeper == null) {
	    
            System.err.println("You must pass at least 2 required parameters: <port> <zooKeeper>");
            
	    String optionalParams = "Optional parameters: %s<corePoolSize> %s<maxPoolSize> %s<queueSize> "
                    + "%s<keepAliveInMillis> %s<maxRequestLen>";
            System.err.println(String.format(optionalParams, C, M, Q, K, R));
            
            String defaultOptions = "Default options: %s%d %s%d %s%d %s%d %s%d";
            System.err.println(String.format(defaultOptions, C, corePoolSize, M, maxPoolSize, Q, queueSize, 
                    K, keepAliveInMillis, R, maxRequestLen));
            
	    System.exit(1);
	}
        
        testXmx(maxRequestLen, corePoolSize);
        
        ProxyServer proxyServer = new ProxyServer(null, port, zooKeeper, corePoolSize, maxPoolSize, 
                keepAliveInMillis, queueSize);

        proxyServer.setMaxRequestLen(maxRequestLen);
        
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = new ObjectName("com.socialbakers.phoenix.proxy.server:type=ProxyServerMBean");
        mbs.registerMBean(proxyServer, name);
        
        proxyServer.startServer();
    }
    
    private static void testXmx(int maxRequestLen, int corePoolSize) {
        debug(String.format("Max request len is %dbytes", maxRequestLen));
        long m = maxRequestLen * corePoolSize;
        long m2 = Runtime.getRuntime().maxMemory();
        if (m > m2) {
            String msg = "Max request len is too high. Increase heap space with jvm param -Xmx<megabytes>m option."
                    + " Or set lower max request size or core pool size."; 
            error(msg, new OutOfMemoryError(msg));
        }
    }

}
