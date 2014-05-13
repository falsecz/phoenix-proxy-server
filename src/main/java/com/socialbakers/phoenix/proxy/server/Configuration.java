package com.socialbakers.phoenix.proxy.server;

public class Configuration {

	public class ConfigurationException extends IllegalArgumentException {

		private static final long serialVersionUID = 1L;

		public ConfigurationException() {
			super();
		}

		public ConfigurationException(String s) {
			super(s);
		}

		public ConfigurationException(String message, Throwable cause) {
			super(message, cause);
		}

		public ConfigurationException(Throwable cause) {
			super(cause);
		}
	}

	// Environment variables
	private static final String ZE = "PHOENIX_ZK"; // zooKeeper jdbc url
	private static final String PE = "PORT"; // port
	private static final String CE = "CORE_POOL_SIZE"; // core pool size
	private static final String ME = "MAX_POOL_SIZE"; // max pool size
	private static final String QE = "QUEUE_SIZE"; // queue size
	private static final String KE = "KEEP_ALIVE_TIME"; // keep alive time in milliseconds
	private static final String RE = "MAX_REQUEST_LEN"; // maximum length of request message in bytes
	private static final String TE = "MAX_CONN_RETRIES";// maximum retries to get zooKeeper connection

	// Program parameters (Overides Env vars)
	private static final String C = "-c"; // core pool size
	private static final String M = "-m"; // max pool size
	private static final String Q = "-q"; // queue size
	private static final String K = "-k"; // keep alive time in milliseconds
	private static final String R = "-r"; // maximum length of request message in bytes
	private static final String T = "-t"; // maximum retries to get zooKeeper connection

	// Configuration values
	private String zooKeeper = null;
	private Integer port = null;
	private int corePoolSize = 128;
	private int maxPoolSize = 128;
	private int queueSize = 20000;
	private int keepAliveInMillis = 20000;

	public Configuration(String[] args) throws ConfigurationException {

		if (System.getenv(ZE) != null) {
			zooKeeper = System.getenv(ZE);
		} else if (System.getenv(PE) != null) {
			port = Integer.valueOf(System.getenv(PE));
		} else if (System.getenv(CE) != null) {
			corePoolSize = Integer.valueOf(System.getenv(CE));
		} else if (System.getenv(ME) != null) {
			maxPoolSize = Integer.valueOf(System.getenv(ME));
		} else if (System.getenv(QE) != null) {
			queueSize = Integer.valueOf(System.getenv(QE));
		} else if (System.getenv(KE) != null) {
			keepAliveInMillis = Integer.valueOf(System.getenv(KE));
		} else if (System.getenv(RE) != null) {
			RequestFilter.maxRequestLen = Integer.valueOf(System.getenv(RE));
		} else if (System.getenv(TE) != null) {
			QueryProcessor.maxRetries = Integer.valueOf(System.getenv(TE));
		}

		if (args.length > 0 && !args[0].startsWith("-")) {
			port = Integer.valueOf(args[0]);
		}
		if (args.length > 1 && !args[1].startsWith("-")) {
			zooKeeper = args[1];
		}

		for (String arg : args) {
			if (arg.startsWith(C)) {
				corePoolSize = Integer.valueOf(arg.replaceFirst(C, ""));
			} else if (arg.startsWith(M)) {
				maxPoolSize = Integer.valueOf(arg.replaceFirst(M, ""));
			} else if (arg.startsWith(Q)) {
				queueSize = Integer.valueOf(arg.replaceFirst(Q, ""));
			} else if (arg.startsWith(K)) {
				keepAliveInMillis = Integer.valueOf(arg.replaceFirst(K, ""));
			} else if (arg.startsWith(R)) {
				RequestFilter.maxRequestLen = Integer.valueOf(arg.replaceFirst(R, ""));
			} else if (arg.startsWith(T)) {
				QueryProcessor.maxRetries = Integer.valueOf(arg.replaceFirst(T, ""));
			}
		}

		if (port == null || zooKeeper == null) {

			String optionalParams = "Optional parameters: %s<corePoolSize> %s<maxPoolSize> %s<queueSize> "
					+ "%s<keepAliveInMillis> %s<maxRequestLen> %s<maxRetries>";

			String defaultOptions = "Default options: %s%d %s%d %s%d %s%d %s%d %s%d";

			StringBuilder msg = new StringBuilder();
			msg.append("You must pass at least 2 required parameters: <port> <zooKeeper>");
			msg.append("\n");
			msg.append(String.format(optionalParams, C, M, Q, K, R, T));
			msg.append("\n");
			msg.append(String.format(defaultOptions, C, corePoolSize, M, maxPoolSize, Q, queueSize, K,
					keepAliveInMillis, R, RequestFilter.maxRequestLen, T, QueryProcessor.maxRetries));

			throw new ConfigurationException(msg.toString());
		}

		testXmx(corePoolSize);
	}

	public int getCorePoolSize() {
		return corePoolSize;
	}

	public int getKeepAliveInMillis() {
		return keepAliveInMillis;
	}

	public int getMaxPoolSize() {
		return maxPoolSize;
	}

	public Integer getPort() {
		return port;
	}

	public int getQueueSize() {
		return queueSize;
	}

	public String getZooKeeper() {
		return zooKeeper;
	}

	public void setCorePoolSize(int corePoolSize) {
		this.corePoolSize = corePoolSize;
	}

	public void setKeepAliveInMillis(int keepAliveInMillis) {
		this.keepAliveInMillis = keepAliveInMillis;
	}

	public void setMaxPoolSize(int maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}

	public void setZooKeeper(String zooKeeper) {
		this.zooKeeper = zooKeeper;
	}

	private void testXmx(int corePoolSize) {
		long m = RequestFilter.maxRequestLen * corePoolSize;
		long m2 = Runtime.getRuntime().maxMemory();
		if (m > m2) {
			String msg = "Max request len is too high. Increase heap space with jvm param -Xmx<megabytes>m option."
					+ " Or set lower max request size or core pool size.";
			throw new ConfigurationException(msg);
		}
	}

}
