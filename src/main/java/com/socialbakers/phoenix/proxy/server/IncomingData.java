/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.socialbakers.phoenix.proxy.server;

import com.socialbakers.phoenix.proxy.PhoenixProxyProtos;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.eclipse.jdt.internal.core.Assert;

/**
 *
 * @author robert
 */
class IncomingData {

    boolean lenRead;
    int len;
    int read;
    byte[] data;

    IncomingData() {
        this.lenRead = false;
        this.len = 4;
        this.read = 0;
        this.data = new byte[len];
    }
    
    synchronized void read(byte[] buf, int size) {
        System.arraycopy(buf, 0, data, read, size);
        read += size;
        
        if (!lenRead && len == read) {
            // just read len of data
            ByteBuffer wrapped = ByteBuffer.wrap(data);
	    len = wrapped.getInt();
            data = new byte[len];
            read = 0;
            lenRead = true;
        }
        
        Assert.isTrue(len >= read, "Data len overflow!");
    }

    synchronized int left() {
        return len - read;
    }

    synchronized boolean isComplete() {
        return lenRead && len == read;
    }

    PhoenixProxyProtos.QueryRequest toQueryRequest() throws IOException {
        return PhoenixProxyProtos.QueryRequest.newBuilder()
                .mergeFrom(data)
                .build();
    }
}
