package com.socialbakers.phoenix.proxy.server;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.socialbakers.phoenix.proxy.Configuration;
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

	private ThreadPoolExecutor executor;

	public RequestHandler(Configuration conf) throws SQLException {
		this.queryProcessor = new QueryProcessor(conf.getZooKeeper());
		RejectedExecutionHandler rejectedExecutionHandler = new RejectedExecutionHandler() {
			@Override
			public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
				((RejectableRunnable) r).reject();
			}
		};
		this.executor = new ThreadPoolExecutor(conf.getCorePoolSize(), conf.getMaxPoolSize(), conf.getKeepAliveTime(),
				TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(conf.getQueueSize()),
				Executors.defaultThreadFactory(), rejectedExecutionHandler);
	}

	@Override
	public void exceptionCaught(IoSession session, Throwable cause) throws Exception {

		synchronized (this) {
			if (cause instanceof IOException && "Connection reset by peer".equals(cause.getMessage())) {
				// nothing to do with peer
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

		PhoenixProxyProtos.QueryRequest request = (PhoenixProxyProtos.QueryRequest) message;

		RejectableRunnable runnable = new RejectableRunnable(session, request);
		executor.execute(runnable);
	}

	@Override
	public void messageSent(IoSession session, Object message) throws Exception {
		synchronized (this) {
			activeRequestsCounter--;
		}
	}

	@Override
	public void sessionClosed(IoSession session) throws Exception {
		synchronized (this) {
			activeConnectionsCounter--;
		}
	}

	@Override
	public void sessionOpened(IoSession session) throws Exception {
		synchronized (this) {
			activeConnectionsCounter++;
			allConnectionsCounter++;
		}
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

	private class RejectableRunnable implements Runnable {
		private IoSession session;
		private PhoenixProxyProtos.QueryRequest request;

		private RejectableRunnable(IoSession session, PhoenixProxyProtos.QueryRequest request) {
			this.session = session;
			this.request = request;
		}

		@Override
		public void run() {
			try {
				QueryResponse response = process();
				synchronized (session) {
					session.write(response);
				}
			} catch (Exception e) {
				throw new IllegalStateException(e.getMessage(), e);
			}
		}

		private QueryResponse process() throws Exception {
			synchronized (this) {
				activeRequestsCounter++;
				allRequestsCounter++;
			}
			return queryProcessor.sendQuery(request);
		}

		private void reject() {
			PhoenixProxyProtos.QueryResponse.Builder responseBuilder = PhoenixProxyProtos.QueryResponse.newBuilder();
			QueryException exception = QueryException.newBuilder().setMessage("Rejected request").build();
			responseBuilder.setException(exception);
			responseBuilder.setCallId(request.getCallId());
			PhoenixProxyProtos.QueryResponse response = responseBuilder.build();

			synchronized (session) {
				session.write(response);
			}
		}
	}

}
