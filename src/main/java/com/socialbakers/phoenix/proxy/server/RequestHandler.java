package com.socialbakers.phoenix.proxy.server;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
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

	private static int getPriority(Runnable r) {
		PriorityRejectableTask task = getPRTaskFromRunnable(r);
		if (task == null) {
			return 0;
		}
		return task.getPriority();
	}

	private static PriorityRejectableTask getPRTaskFromRunnable(Runnable r) {
		if (r instanceof PriorityRejectableTask) {
			return (PriorityRejectableTask) r;
			// } else if (callableField != null && r instanceof FutureTask) {
			// try {
			// callableField.get(r);
			// } catch (Exception e) {
			// return null;
			// }
		}
		return null;
	}

	private QueryProcessor queryProcessor;

	private int activeConnectionsCounter = 0;
	private int allConnectionsCounter = 0;
	private int activeRequestsCounter = 0;
	private int allRequestsCounter = 0;
	private int rejectionsCounter = 0;
	private int errorCounter = 0;

	private ThreadPoolExecutor executor;
	private PriorityBlockingQueue<Runnable> queue;

	private static Field callableField;

	static {
		try {
			callableField = FutureTask.class.getDeclaredField("callable");
			callableField.setAccessible(true);
		} catch (Exception e) {
			try {
				Field sync = FutureTask.class.getDeclaredField("sync");
				sync.setAccessible(true);
				callableField = sync.getType().getDeclaredField("callable");
				callableField.setAccessible(true);
			} catch (Exception e1) {
				callableField = null;
			}
		}
	}

	public RequestHandler(Configuration conf) throws SQLException {
		RejectedExecutionHandler rejectedExecutionHandler = new RejectedExecutionHandler() {
			@Override
			public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
				PriorityRejectableTask task = getPRTaskFromRunnable(r);
				if (task != null) {
					task.reject();
				}
			}
		};

		Comparator<Runnable> priorityComparator = new Comparator<Runnable>() {
			@Override
			public int compare(Runnable o1, Runnable o2) {
				return getPriority(o2) - getPriority(o1);
			}
		};

		this.queue = new PriorityBlockingQueue<Runnable>(conf.getQueueSize(), priorityComparator);

		this.executor = new ThreadPoolExecutor(conf.getCorePoolSize(), conf.getMaxPoolSize(), conf.getKeepAliveTime(),
				TimeUnit.SECONDS, queue, Executors.defaultThreadFactory(), rejectedExecutionHandler) {

			@Override
			protected void afterExecute(Runnable r, Throwable t) {
				if (t == null && r instanceof Future<?>) {
					try {
						Future<?> future = (Future<?>) r;
						if (future.isDone()) {
							future.get();
						}
					} catch (CancellationException ce) {
						t = ce;
					} catch (ExecutionException ee) {
						t = ee.getCause();
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt(); // ignore/reset
					}
				}
				if (t != null) {
					LOGGER.error(t.getMessage(), t);
				}
			}

			@Override
			protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
				return new RejectablePriorityFutureTask<T>(callable);
			}

		};
		this.queryProcessor = new QueryProcessor(conf.getZooKeeper(), executor);
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

	public ThreadPoolExecutor getExecutor() {
		return executor;
	}

	public synchronized int getRejectionsCounter() {
		return rejectionsCounter;
	}

	public int getTaskCountInQueue() {
		return queue.size();
	}

	@Override
	public void messageReceived(IoSession session, Object message) throws Exception {

		if (!(message instanceof PhoenixProxyProtos.QueryRequest)) {
			throw new IllegalArgumentException("Message must be an instance of "
					+ PhoenixProxyProtos.QueryRequest.class.getName());
		}

		PhoenixProxyProtos.QueryRequest request = (PhoenixProxyProtos.QueryRequest) message;

		RequestTask runnable = new RequestTask(session, request);
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

	private class RejectablePriorityFutureTask<T> extends FutureTask<T> implements PriorityRejectableTask {
		private PriorityRejectableTask task;

		public RejectablePriorityFutureTask(Callable<T> callable) {
			super(callable);
			if (callable instanceof PriorityRejectableTask) {
				this.task = (PriorityRejectableTask) callable;
			}
		}

		@Override
		public int getPriority() {
			return task != null ? task.getPriority() : 0;
		}

		@Override
		public void reject() {
			if (task != null) {
				task.reject();
			}
		}
	}

	private class RequestTask implements Runnable, PriorityRejectableTask {

		private IoSession session;
		private PhoenixProxyProtos.QueryRequest request;
		private int priority;

		private RequestTask(IoSession session, PhoenixProxyProtos.QueryRequest request) {
			this.session = session;
			this.request = request;
			// zero priority for batch requests, max(2) priority to single query requests.
			// batch queries in already started tasks has priority 1
			this.priority = request.getQueriesCount() < 2 ? 2 : 0;
		}

		@Override
		public int getPriority() {
			return priority;
		}

		@Override
		public void reject() {
			PhoenixProxyProtos.QueryResponse.Builder responseBuilder = PhoenixProxyProtos.QueryResponse.newBuilder();
			QueryException exception = QueryException.newBuilder().setMessage("Rejected request").build();
			responseBuilder.setException(exception);
			responseBuilder.setCallId(request.getCallId());
			PhoenixProxyProtos.QueryResponse response = responseBuilder.build();

			synchronized (session) {
				session.write(response);
			}
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
	}

}
