package com.socialbakers.phoenix.proxy.server;

public interface ProxyServerMBean {
    
    int getActiveConnections();
    
    int getActiveRequests();
    
    int getRequestsInQueue();
    
    
    int getPoolSize();
    
    int getCorePoolSize();
    
    int getMaxPoolSize();
    
    int getQueueSize();
    
    
    long getConnectionCount();
    
    long getRequestCount();
    
    long getErrorCount();
    
    long getRejectionCount();
    
    
    void setMaxRequestLen(int maxRequestLen);
    
    int getMaxRequestLen();
    
    void setMaxRetries(int maxRetries);
    
    int getMaxRetries();
    
    
    void resetCounters();
    
}
