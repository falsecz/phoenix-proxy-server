package com.socialbakers.phoenix.proxy.server;

//import com.socialbakers.phoenix.proxy.Phoenix;

import com.socialbakers.phoenix.proxy.PhoenixProxyProtos;




/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
	PhoenixProxyProtos.QueryRequest.Builder x = PhoenixProxyProtos.QueryRequest.newBuilder();
	// x.mergeFrom(buffer)
        System.out.println( "Hello World!" );
    }
}
