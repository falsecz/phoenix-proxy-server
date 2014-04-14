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
class RequestProcessor implements Runnable {

	private SelectionKey key;
	private PhoenixProxyProtos.QueryRequest queryRequest;
        private QueryProcessor queryProcessor;
        
	RequestProcessor(SelectionKey key, PhoenixProxyProtos.QueryRequest queryRequest, 
                QueryProcessor queryProcessor) {
	    this.key = key;
	    this.queryRequest = queryRequest;
            this.queryProcessor = queryProcessor;
	}

        SocketChannel getChannel() {
            return (SocketChannel) key.channel();
        }
        
	@Override
	public void run() {

	    try {
		// JDBC read data
		PhoenixProxyProtos.QueryResponse response = queryProcessor.sendQuery(queryRequest);

		// data to bytes
		byte[] data = response.toByteArray();

		int len = data.length;
//		ByteBuffer lenBuf = ByteBuffer.allocate(4);
//		lenBuf.putInt(len);
//		byte[] lenBytes = lenBuf.array();
                
		// response
		System.out.println("response len: " + data.length);
		SocketChannel channel = getChannel();
//		byte[] data2 = new byte[data.length + lenBytes.length];
//		System.arraycopy(lenBytes, 0, data2, 0, lenBytes.length);
//		System.arraycopy(data, 0, data2, lenBytes.length, data.length);
                
                ByteBuffer wrap = ByteBuffer.allocate(data.length + 4);
                wrap.putInt(len);
                wrap.put(data);
                wrap.flip();
                
		System.out.println("wrap len: " + wrap.array().length);
                while (wrap.hasRemaining()) {
                    int write = channel.write(wrap);
                    System.out.println("send: " + write);
                }
//                doEcho(key, lenBytes);
//                doEcho(key, data);
	    } catch (IOException e) {
		e.printStackTrace();
	    } catch (Exception e) {
		e.printStackTrace();
//                Logger.getLogger(EchoServer2.class.getName()).log(Level.SEVERE, null, ex);
	    }
	}
    }