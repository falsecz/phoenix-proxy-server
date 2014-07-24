package com.socialbakers.phoenix.proxy.server;

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.mina.core.filterchain.DefaultIoFilterChainBuilder;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.filter.executor.IoEventQueueThrottle;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.socialbakers.config.exception.ConfigurationException;
import com.socialbakers.phoenix.proxy.Configuration;

public class ProxyServer {

	private static final Logger LOGGER = LoggerFactory.getLogger(ProxyServer.class);

	public static void main(String[] args) throws Exception {

		try {

			Configuration conf = new Configuration(args);

			IoAcceptor acceptor = new NioSocketAcceptor();
			DefaultIoFilterChainBuilder filterChain = acceptor.getFilterChain();

			filterChain.addLast("request", new RequestFilter());
			filterChain.addLast("logger", new LoggingFilter());

			IoEventQueueThrottle queueHandler = new IoEventQueueThrottle(conf.getQueueSize());
			ExecutorFilter threadPoolExecutor = new ExecutorFilter(conf.getCorePoolSize(), conf.getMaxPoolSize(),
					conf.getKeepAliveTime(), TimeUnit.MILLISECONDS, queueHandler);
			filterChain.addLast("executor", threadPoolExecutor);

			RequestHandler requestHandler = new RequestHandler(conf);
			acceptor.setHandler(requestHandler);

			registerJmx(conf, queueHandler, threadPoolExecutor.getExecutor(), requestHandler);

			acceptor.bind(new InetSocketAddress(conf.getPort()));
			acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 5);

			LOGGER.info("Listening on 0.0.0.0:" + conf.getPort());

		} catch (ConfigurationException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}

	private static void registerJmx(Configuration conf, IoEventQueueThrottle queueHandler, Executor executor,
			RequestHandler requestHandler) throws MalformedObjectNameException, InstanceAlreadyExistsException,
			MBeanRegistrationException, NotCompliantMBeanException {
		Jmx proxyServer = new Jmx(conf, executor, queueHandler, requestHandler);
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		ObjectName name = new ObjectName("com.socialbakers.phoenix.proxy.server:type=ProxyServerMBean");
		mbs.registerMBean(proxyServer, name);
	}

}
