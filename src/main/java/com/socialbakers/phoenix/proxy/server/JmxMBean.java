package com.socialbakers.phoenix.proxy.server;

public interface JmxMBean {

	int getActiveConnections();

	int getActiveRequests();

	long getConnectionCount();

	int getCorePoolSize();

	long getErrorCount();

	int getMaxPoolSize();

	int getMaxRequestLen();

	int getMaxRetries();

	int getPoolSize();

	int getQueueSize();

	long getRejectionCount();

	long getRequestCount();

	int getRequestsInQueue();

	void resetCounters();

	void setMaxRequestLen(int maxRequestLen);

	void setMaxRetries(int maxRetries);

}
