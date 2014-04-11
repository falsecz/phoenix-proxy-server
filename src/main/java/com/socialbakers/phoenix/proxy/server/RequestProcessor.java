/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.socialbakers.phoenix.proxy.server;

import com.socialbakers.phoenix.proxy.PhoenixProxyProtos;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/**
 *
 * @author robert
 */
class RequestProcessor extends Thread {

	private SelectionKey key;
	private IncomingData message;
        private QueryProcessor queryProcessor;
        
	RequestProcessor(SelectionKey key, IncomingData message, 
                QueryProcessor queryProcessor) {
	    this.key = key;
	    this.message = message;
            this.queryProcessor = queryProcessor;
	}

	@Override
	public void run() {

	    try {

		PhoenixProxyProtos.QueryRequest queryRequest = message.toQueryRequest();

		int callId = queryRequest.getCallId();
		String query = queryRequest.getQuery();
		PhoenixProxyProtos.QueryRequest.Type type = queryRequest.getType();
//		log("Query: " + query);

		// JDBC read data
		PhoenixProxyProtos.QueryResponse response = queryProcessor.sendQuery(query, callId, type);

		// data to bytes
		byte[] data = response.toByteArray();

		int len = data.length;
		ByteBuffer lenBuf = ByteBuffer.allocate(4);
		lenBuf.putInt(len);
		byte[] lenBytes = lenBuf.array();

		// response
//		log("response len: " + data.length);
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