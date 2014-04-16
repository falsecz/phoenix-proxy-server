package com.socialbakers.phoenix.proxy.server;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

class RequestPool extends ThreadPoolExecutor {
    
    private int queueSize;
    
    /**
     * Creates a new {@code RequestPool} with the given initial
     * parameters.
     * @param corePoolSize the number of threads to keep in the pool, even
     *        if they are idle, unless {@code allowCoreThreadTimeOut} is set
     * @param maximumPoolSize the maximum number of threads to allow in the
     *        pool
     * @param keepAliveTime when the number of threads is greater than
     *        the core, this is the maximum time in milliseconds that excess idle threads
     *        will wait for new tasks before terminating.
     * @param queueSize the capacity of queue
     * @param rejectionHandler the handler to use when execution is blocked
     *        because the thread bounds and queue capacities are reached
     */
    RequestPool(int corePoolSize, int maximumPoolSize, long keepAliveTimeMs, int queueSize,
            RejectedExecutionHandler rejectionHandler) {
        
        super(corePoolSize, maximumPoolSize, keepAliveTimeMs, TimeUnit.MILLISECONDS, 
                new ArrayBlockingQueue<Runnable>(queueSize), Executors.defaultThreadFactory(), rejectionHandler);
        
        this.queueSize = queueSize;
    }
    
    /**
     * Executes the given task sometime in the future.  The task
     * may execute in a new thread or in an existing pooled thread.
     *
     * If the task cannot be submitted for execution, either because this
     * executor has been shutdown or because its capacity has been reached,
     * the task is handled by the current {@code RejectedExecutionHandler}.
     *
     * @param command the task to execute
     * @throws RejectedExecutionException at discretion of
     *         {@code RejectedExecutionHandler}, if the task
     *         cannot be accepted for execution
     * @throws NullPointerException if {@code command} is null
     */
    void execute(RequestProcessor command) throws RejectedExecutionException {
        super.execute(command);
    }
    
    /**
     * Only {@link RequestProcessor} instance can be executed.
     * @param r runnable command
     */
    @Override
    public void execute(Runnable r) {
        throw new UnsupportedOperationException("Only " + RequestProcessor.class.getName() 
                + " instance can be executed.");
    }

    /**
     * Returns the count of commands waiting in queue.
     * @return the count of commands waiting in queue
     */
    int getCmdCountInQueue() {
        return getQueue().size();
    }

    /**
     * @return queue size
     */
    int getQueueSize() {
        return queueSize;
    }
}
