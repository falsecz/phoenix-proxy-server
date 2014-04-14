/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.socialbakers.phoenix.proxy.server;

import com.socialbakers.phoenix.proxy.PhoenixProxyProtos;
import java.nio.channels.SelectionKey;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author robert
 */
class RequestProcessor implements Runnable {

    private static final Logger logger = Logger.getLogger(RequestProcessor.class.getName());

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

    interface SocketWriter {
        void write(SelectionKey key, byte[] bytes);
    }
}
