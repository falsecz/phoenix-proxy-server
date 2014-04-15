/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.socialbakers.phoenix.proxy.server;

import static com.socialbakers.phoenix.proxy.server.Logger.log;

import com.socialbakers.phoenix.proxy.PhoenixProxyProtos;
import java.nio.channels.SelectionKey;

/**
 *
 * @author robert
 */
class RequestProcessor implements Runnable {

    private SelectionKey key;
    private PhoenixProxyProtos.QueryRequest queryRequest;
    private QueryProcessor queryProcessor;
    private SocketWriter writer;

    RequestProcessor(SelectionKey key, PhoenixProxyProtos.QueryRequest queryRequest,
            QueryProcessor queryProcessor, SocketWriter writer) {
        this.key = key;
        this.queryRequest = queryRequest;
        this.queryProcessor = queryProcessor;
        this.writer = writer;
    }

    SelectionKey getKey() {
        return key;
    }

    PhoenixProxyProtos.QueryResponse createExceptionResponse(String message) {
        PhoenixProxyProtos.QueryResponse.Builder responseBuilder = PhoenixProxyProtos.QueryResponse.newBuilder();
        responseBuilder.setCallId(queryRequest.getCallId());
        PhoenixProxyProtos.QueryException exception = PhoenixProxyProtos.QueryException.newBuilder()
                .setMessage(message)
                .build();
        responseBuilder.setException(exception);
        PhoenixProxyProtos.QueryResponse response = responseBuilder.build();
        return response;
    }

    @Override
    public void run() {

        try {
            // JDBC read data
            PhoenixProxyProtos.QueryResponse response = queryProcessor.sendQuery(queryRequest);

            // data to bytes
            byte[] data = response.toByteArray();
            writer.write(key, data);

        } catch (Exception e) {
            log(e);
        }
    }

    interface SocketWriter {
        void write(SelectionKey key, byte[] bytes);
    }
}
