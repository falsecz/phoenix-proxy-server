package com.socialbakers.phoenix.proxy.server;

//import com.socialbakers.phoenix.proxy.Phoenix;

import com.socialbakers.phoenix.proxy.PhoenixProxyProtos;
import java.io.IOException;




/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) throws IOException {
	PhoenixProxyProtos.QueryRequest.Builder x = PhoenixProxyProtos.QueryRequest.newBuilder();
        x = x.mergeFrom(new byte[]{});
        
        
        EchoServer server = new EchoServer(null, 8989);
        server.start();
        System.out.println("Server started asynch!");
    }
}
