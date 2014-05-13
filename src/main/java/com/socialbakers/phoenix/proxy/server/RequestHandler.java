package com.socialbakers.phoenix.proxy.server;

import java.io.IOException;
import java.util.concurrent.RejectedExecutionException;

import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.socialbakers.phoenix.proxy.PhoenixProxyProtos;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos.QueryResponse;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos.QueryResponse.QueryException;

public class RequestHandler extends IoHandlerAdapter {

	private static final Logger LOGGER = LoggerFactory.getLogger(RequestHandler.class);

	private QueryProcessor queryProcessor;

	private int activeConnectionsCounter = 0;
	private int allConnectionsCounter = 0;
	private int activeRequestsCounter = 0;
	private int allRequestsCounter = 0;
	private int rejectionsCounter = 0;
	private int errorCounter = 0;

	public RequestHandler(QueryProcessor queryProcessor) {
		super();
		this.queryProcessor = queryProcessor;
	}

	@Override
	public void exceptionCaught(IoSession session, Throwable cause) throws Exception {

		synchronized (this) {
			if (cause instanceof IOException && "Connection reset by peer".equals(cause.getMessage())) {
				return;
			} else if (cause instanceof RejectedExecutionException) {
				rejectionsCounter++;
			} else {
				errorCounter++;
			}
		}

		try {
			PhoenixProxyProtos.QueryResponse.Builder responseBuilder = PhoenixProxyProtos.QueryResponse.newBuilder();
			QueryException exception = QueryException.newBuilder().setMessage(cause.getMessage()).build();
			responseBuilder.setException(exception);
			responseBuilder.setCallId(-1);
			PhoenixProxyProtos.QueryResponse response = responseBuilder.build();
			session.write(response);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			try {
				session.close(false);
			} catch (Exception e) {
			}
		}
	}

	public synchronized int getActiveConnectionsCounter() {
		return activeConnectionsCounter;
	}

	public synchronized int getActiveRequestsCounter() {
		return activeRequestsCounter;
	}

	public synchronized int getAllConnectionsCounter() {
		return allConnectionsCounter;
	}

	public synchronized int getAllRequestsCounter() {
		return allRequestsCounter;
	}

	public synchronized int getErrorCounter() {
		return errorCounter;
	}

	public synchronized int getRejectionsCounter() {
		return rejectionsCounter;
	}

	@Override
	public void messageReceived(IoSession session, Object message) throws Exception {

		if (!(message instanceof PhoenixProxyProtos.QueryRequest)) {
			throw new IllegalArgumentException("Message must be an instance of "
					+ PhoenixProxyProtos.QueryRequest.class.getName());
		}

		synchronized (this) {
			activeRequestsCounter++;
			allRequestsCounter++;
		}

		PhoenixProxyProtos.QueryRequest request = (PhoenixProxyProtos.QueryRequest) message;
		QueryResponse response = queryProcessor.sendQuery(request);
		session.write(response);
	}

	@Override
	public void messageSent(IoSession session, Object message) throws Exception {

		synchronized (this) {
			activeRequestsCounter--;
		}

		super.messageSent(session, message);
	}

	@Override
	public void sessionClosed(IoSession session) throws Exception {

		synchronized (this) {
			activeConnectionsCounter--;
		}

		super.sessionClosed(session);
	}

	@Override
	public void sessionOpened(IoSession session) throws Exception {

		synchronized (this) {
			activeConnectionsCounter++;
			allConnectionsCounter++;
		}

		super.sessionOpened(session);
	}

	public synchronized void setAllConnectionsCounter(int allConnectionsCounter) {
		this.allConnectionsCounter = allConnectionsCounter;
	}

	public synchronized void setAllRequestsCounter(int allRequestsCounter) {
		this.allRequestsCounter = allRequestsCounter;
	}

	public synchronized void setErrorCounter(int errorCounter) {
		this.errorCounter = errorCounter;
	}

	public synchronized void setRejectionsCounter(int rejectionsCounter) {
		this.rejectionsCounter = rejectionsCounter;
	}

}
