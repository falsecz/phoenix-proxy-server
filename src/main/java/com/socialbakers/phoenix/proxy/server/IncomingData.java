package com.socialbakers.phoenix.proxy.server;

import com.socialbakers.phoenix.proxy.PhoenixProxyProtos;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.eclipse.jdt.internal.core.Assert;

/**
 * Represents data of one query request.
 * @author robert
 */
class IncomingData {
    
    static int maxRequestLen = 10 * 1024 * 1024; // 10 MB
    
    /**
     * Flag for length of request was read.
     * If its false message is in state1 - waiting for read the length of message.
     * If its true message is in state2 - waiting for read the rest of message.
     */
    boolean lenRead;
    
    /**
     * Length of data to read in actual state.
     * 4 bytes in first state. X bytes in second state.
     */
    int len;
    
    /**
     * Number of bytes read in this state.
     */
    int read;
    
    /**
     * Data buffer.
     */
    byte[] data;

    /**
     * Default and the only one constructor.
     */
    IncomingData() {
        this.lenRead = false;
        this.len = 4;
        this.read = 0;
        this.data = new byte[len];
    }
    
    /**
     * Read some bytes to data buffer.
     * @param buf data to read
     * @param size number of bytes to read
     */
    synchronized void read(byte[] buf, int size) {

        Assert.isTrue(len >= (read + size), "Data len overflow!");
        
        System.arraycopy(buf, 0, data, read, size);
        read += size;
        
        if (!lenRead && len == read) {
            // just read len of data - state1 -> state2
            ByteBuffer wrapped = ByteBuffer.wrap(data);
	    len = wrapped.getInt();
            
            if (len <= 0) {
                throw new IllegalStateException("Message length must be greater than zero!");
            } else if (len > maxRequestLen) {
                throw new IllegalStateException(String.format("Message length must be lower or equal than %d!", 
                        maxRequestLen));
            }

            data = new byte[len];
            read = 0;
            lenRead = true;
        }
    }

    /**
     * @return number of bytes to read in this state.
     */
    synchronized int left() {
        return len - read;
    }

    /**
     * @return true if whole message was read
     */
    synchronized boolean isComplete() {
        return lenRead && len == read;
    }
    
    /**
     * Converts message to QueryRequest object.
     * @return QueryRequest object.
     * @throws IOException
     */
    PhoenixProxyProtos.QueryRequest toQueryRequest() throws IOException {
        
        Assert.isTrue(isComplete(), "Incomplete data!");
        
        return PhoenixProxyProtos.QueryRequest.newBuilder()
                .mergeFrom(data)
                .build();
    }
}
