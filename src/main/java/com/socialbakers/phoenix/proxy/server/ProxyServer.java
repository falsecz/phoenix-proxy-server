package com.socialbakers.phoenix.proxy.server;

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
import java.sql.SQLException;
import java.util.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
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
    private RequestPool requestPool;
    
    private RequestProcessor.SocketWriter writer = new RequestProcessor.SocketWriter() {
        @Override
        public void write(SelectionKey key, byte[] bytes) {
//            doEcho(key, bytes);
            synchronized (key) {
                SocketChannel channel = (SocketChannel) key.channel();
                ByteBuffer wrap = ByteBuffer.wrap(bytes);
                while (wrap.hasRemaining()) {
                    try {
                        channel.write(wrap);
                    } catch (IOException ex) {
                        try {
                            closeChannel(key);
                        } catch (IOException ex1) {
                            Logger.getLogger(ProxyServer.class.getName()).log(Level.SEVERE, null, ex1);
                        }
                        return;
                    }
                }
            }
        }
    };
    
    private RejectedExecutionHandler rejectionHandler = new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            lenBuf.putInt(0);
            byte[] zero = lenBuf.array();
            SelectionKey key = ((RequestProcessor) r).getKey();
            writer.write(key, zero);
            System.out.println(r.toString() + " is rejected, sendind ZERO to client.");
        }
    };
    
    public ProxyServer(InetAddress addr, int port, String zooKeeper,
            int corePoolSize, int maximumPoolSize, long keepAliveTimeMs, int queueSize) throws SQLException {
	
        this.addr = addr;
	this.port = port;
	this.requestPool = new RequestPool(corePoolSize, maximumPoolSize, keepAliveTimeMs, queueSize, 
                writer, rejectionHandler);
        
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
//	    this.selector.select();

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
            closeChannel(key);
	    return;
	}

	if (message == null) {
	    // new request
	    byte[] data = new byte[numRead];
	    System.arraycopy(buffer.array(), 0, data, 0, numRead);
	    ByteBuffer wrapped = ByteBuffer.wrap(data);
	    int len = wrapped.getInt();
//	    log("Got len of new message: " + len);
	    message = new IncomingData(len);
	    incomingData.put(channel, message);
	} else {
	    // continue reading
	    message.read(buffer.array(), numRead);
	}

	if (message.isComplete()) {
	    incomingData.remove(channel);
            PhoenixProxyProtos.QueryRequest queryRequest = message.toQueryRequest();
	    RequestProcessor processor = new RequestProcessor(key, queryRequest, queryProcessor, writer);
	    requestPool.execute(processor);
	}
    }
    
    private void closeChannel(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        this.outgoingData.remove(channel);
        this.incomingData.remove(channel);
        Socket socket = channel.socket();
        SocketAddress remoteAddr = socket.getRemoteSocketAddress();
        log("Connection closed by client: " + remoteAddr);
        channel.close();
        key.cancel();
    }
    
    private void write(SelectionKey key) throws IOException {
        
	SocketChannel channel = (SocketChannel) key.channel();
	List<byte[]> pendingData = this.outgoingData.get(channel);
	Iterator<byte[]> items = pendingData.iterator();
        
        while (items.hasNext()) {
            byte[] item = items.next();
            items.remove();
            ByteBuffer wrap = ByteBuffer.wrap(item);
            while (wrap.hasRemaining()) {
                channel.write(wrap);
            }
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
    

    /**
     * Sends ZERO to client if command was rejected.
     */
    private class RejectedExecutionHandlerImpl implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            lenBuf.putInt(0);
            byte[] zero = lenBuf.array();
            SelectionKey key = ((RequestProcessor) r).getKey();
            writer.write(key, zero);
            System.out.println(r.toString() + " is rejected, sendind ZERO to client.");
        }
    }
    
    
    private static final String C = "-c"; // core pool size
    private static final String M = "-m"; // max pool size
    private static final String Q = "-q"; // queue size
    private static final String K = "-k"; // keep alive time in milliseconds

    
    private static final String ZE = "PHOENIX_ZK";      // core pool size
    private static final String PE = "PORT";            // core pool size
    private static final String CE = "CORE_POOL_SIZE";  // core pool size
    private static final String ME = "MAX_POOL_SIZE";   // max pool size
    private static final String QE = "QUEUE_SIZE";      // queue size
    private static final String KE = "KEEP_ALIVE_TIME"; // keep alive time in milliseconds

    
    public static void main(String[] args) throws Exception {

	String zooKeeper = null;
        Integer port = null;
        int corePoolSize = 128;
        int maxPoolSize = 128;
        int queueSize = 20000;
        int keepAliveInMillis = 20000;

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
        
	if ((port == null || zooKeeper == null) && (args.length < 2)) {
	    System.err.println("You must pass at least 2 required parameters: <port> <zooKeeper>");
	    System.err.println("Optional parameters: " + C + "<corePoolSize> " 
                    + M + "<maxPoolSize> " + Q + "<queueSize> " + K + "<keepAliveInMillis>");
	    System.exit(1);
	}
        
	port = Integer.valueOf(args[0]);
	zooKeeper = args[1];
        
        
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
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
        
        ProxyServer proxyServer = new ProxyServer(null, port, zooKeeper, corePoolSize, maxPoolSize, 
                keepAliveInMillis, queueSize);
        proxyServer.startServer();
    }
}