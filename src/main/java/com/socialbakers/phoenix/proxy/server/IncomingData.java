/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.socialbakers.phoenix.proxy.server;

import com.socialbakers.phoenix.proxy.PhoenixProxyProtos;
import java.io.IOException;
import org.eclipse.jdt.internal.core.Assert;

/**
 *
 * @author robert
 */
class IncomingData {

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
        Assert.isTrue(len >= read, "Data len overflow!");
    }

    int left() {
        return len - read;
    }

    boolean isComplete() {
        return len == read;
    }

    PhoenixProxyProtos.QueryRequest toQueryRequest() throws IOException {
        return PhoenixProxyProtos.QueryRequest.newBuilder()
                .mergeFrom(data)
                .build();
    }
}
