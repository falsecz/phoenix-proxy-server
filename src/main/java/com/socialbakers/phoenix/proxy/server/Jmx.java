package com.socialbakers.phoenix.proxy.server;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

import com.socialbakers.phoenix.proxy.Configuration;

public class Jmx implements JmxMBean {

	private Configuration conf;
	private Executor executor;
	private RequestHandler requestHandler;

	public Jmx(Configuration conf, Executor executor, RequestHandler requestHandler) {
		super();
		this.conf = conf;
		this.executor = executor;
		this.requestHandler = requestHandler;
	}

	@Override
	public int getActiveConnections() {
		return requestHandler.getActiveConnectionsCounter();
	}

	@Override
	public int getActiveRequests() {
		if (executor instanceof ThreadPoolExecutor) {
			return ((ThreadPoolExecutor) executor).getActiveCount();
		}
		return -1;
	}

	@Override
	public long getConnectionCount() {
		return requestHandler.getAllConnectionsCounter();
	}

	@Override
	public int getCorePoolSize() {
		return conf.getCorePoolSize();
	}

	@Override
	public long getErrorCount() {
		return requestHandler.getErrorCounter();
	}

	@Override
	public int getMaxPoolSize() {
		return conf.getMaxPoolSize();
	}

	@Override
	public int getMaxRequestLen() {
		return RequestFilter.maxRequestLen;
	}

	@Override
	public int getMaxRetries() {
		return QueryProcessor.maxRetries;
	}

	@Override
	public int getPoolSize() {
		if (executor instanceof ThreadPoolExecutor) {
			return ((ThreadPoolExecutor) executor).getPoolSize();
		}
		return -1;
	}

	@Override
	public int getQueueSize() {
		return conf.getQueueSize();
	}

	@Override
	public long getRejectionCount() {
		return requestHandler.getRejectionsCounter();
	}

	@Override
	public long getRequestCount() {
		return requestHandler.getAllRequestsCounter();
	}

	@Override
	public int getRequestsInQueue() {
		return requestHandler.getTaskCountInQueue();
	}

	@Override
	public synchronized void resetCounters() {
		requestHandler.setAllConnectionsCounter(0);
		requestHandler.setAllRequestsCounter(0);
		requestHandler.setRejectionsCounter(0);
		requestHandler.setErrorCounter(0);
	}

	@Override
	public void setMaxRequestLen(int maxRequestLen) {
		RequestFilter.maxRequestLen = maxRequestLen;
	}

	@Override
	public void setMaxRetries(int maxRetries) {
		QueryProcessor.maxRetries = maxRetries;
	}
}
