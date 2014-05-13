package com.socialbakers.phoenix.proxy.server;

import java.io.IOException;

import org.apache.mina.core.filterchain.IoFilter;
import org.apache.mina.core.filterchain.IoFilterAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoEventType;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.core.write.WriteRequest;
import org.apache.mina.filter.logging.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs all MINA protocol events. Each event can be
 * tuned to use a different level based on the user's specific requirements. Methods
 * are in place that allow the user to use either the get or set method for each event
 * and pass in the {@link IoEventType} and the {@link LogLevel}.
 * 
 * By default, all events are logged to the {@link LogLevel#INFO} level except
 * {@link IoFilterAdapter#exceptionCaught(IoFilter.NextFilter, IoSession, Throwable)},
 * which is logged to {@link LogLevel#WARN}.
 * 
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 * @org.apache.xbean.XBean
 */
public class LoggingFilter extends IoFilterAdapter {

	/** The logger name */
	private final String name;

	/** The logger */
	private final Logger logger;

	/** The log level for the exceptionCaught event. */
	private LogLevel exceptionCaughtLevel = LogLevel.ERROR;

	/** The log level for the messageSent event. */
	private LogLevel messageSentLevel = LogLevel.TRACE;

	/** The log level for the messageReceived event. */
	private LogLevel messageReceivedLevel = LogLevel.TRACE;

	/** The log level for the sessionCreated event. */
	private LogLevel sessionCreatedLevel = LogLevel.TRACE;

	/** The log level for the sessionOpened event. */
	private LogLevel sessionOpenedLevel = LogLevel.DEBUG;

	/** The log level for the sessionIdle event. */
	private LogLevel sessionIdleLevel = LogLevel.TRACE;

	/** The log level for the sessionClosed event. */
	private LogLevel sessionClosedLevel = LogLevel.DEBUG;

	/**
	 * Default Constructor.
	 */
	public LoggingFilter() {
		this(LoggingFilter.class.getName());
	}

	/**
	 * Create a new NoopFilter using a class name
	 * 
	 * @param clazz
	 *            the cass which name will be used to create the logger
	 */
	public LoggingFilter(Class<?> clazz) {
		this(clazz.getName());
	}

	/**
	 * Create a new NoopFilter using a name
	 * 
	 * @param name
	 *            the name used to create the logger. If null, will default to "NoopFilter"
	 */
	public LoggingFilter(String name) {
		if (name == null) {
			this.name = LoggingFilter.class.getName();
		} else {
			this.name = name;
		}

		logger = LoggerFactory.getLogger(this.name);
	}

	@Override
	public void exceptionCaught(NextFilter nextFilter, IoSession session, Throwable cause) throws Exception {
		if (cause instanceof IOException && "Connection reset by peer".equals(cause.getMessage())) {
			log(LogLevel.TRACE, "Connection reset by peer: {}", session.getRemoteAddress());
		} else {
			log(exceptionCaughtLevel, "EXCEPTION :", cause);
		}
		nextFilter.exceptionCaught(session, cause);
	}

	/**
	 * Get the LogLevel for the ExceptionCaught event.
	 * 
	 * @return The LogLevel for the ExceptionCaught eventType
	 */
	public LogLevel getExceptionCaughtLogLevel() {
		return exceptionCaughtLevel;
	}

	/**
	 * Get the LogLevel for the MessageReceived event.
	 * 
	 * @return The LogLevel for the MessageReceived eventType
	 */
	public LogLevel getMessageReceivedLogLevel() {
		return messageReceivedLevel;
	}

	/**
	 * Get the LogLevel for the MessageSent event.
	 * 
	 * @return The LogLevel for the MessageSent eventType
	 */
	public LogLevel getMessageSentLogLevel() {
		return messageSentLevel;
	}

	/**
	 * @return The logger's name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Get the LogLevel for the SessionClosed event.
	 * 
	 * @return The LogLevel for the SessionClosed eventType
	 */
	public LogLevel getSessionClosedLogLevel() {
		return sessionClosedLevel;
	}

	/**
	 * Get the LogLevel for the SessionCreated event.
	 * 
	 * @return The LogLevel for the SessionCreated eventType
	 */
	public LogLevel getSessionCreatedLogLevel() {
		return sessionCreatedLevel;
	}

	/**
	 * Get the LogLevel for the SessionIdle event.
	 * 
	 * @return The LogLevel for the SessionIdle eventType
	 */
	public LogLevel getSessionIdleLogLevel() {
		return sessionIdleLevel;
	}

	/**
	 * Get the LogLevel for the SessionOpened event.
	 * 
	 * @return The LogLevel for the SessionOpened eventType
	 */
	public LogLevel getSessionOpenedLogLevel() {
		return sessionOpenedLevel;
	}

	@Override
	public void messageReceived(NextFilter nextFilter, IoSession session, Object message) throws Exception {
		log(messageReceivedLevel, "RECEIVED from {} : {}", session.getRemoteAddress(), message);
		nextFilter.messageReceived(session, message);
	}

	@Override
	public void messageSent(NextFilter nextFilter, IoSession session, WriteRequest writeRequest) throws Exception {
		log(messageSentLevel, "SENT to {} : {}", session.getRemoteAddress(), writeRequest.getMessage());
		nextFilter.messageSent(session, writeRequest);
	}

	@Override
	public void sessionClosed(NextFilter nextFilter, IoSession session) throws Exception {
		log(sessionClosedLevel, "CLOSED {}", session.getRemoteAddress());
		nextFilter.sessionClosed(session);
	}

	@Override
	public void sessionCreated(NextFilter nextFilter, IoSession session) throws Exception {
		log(sessionCreatedLevel, "CREATED {}", session.getRemoteAddress());
		nextFilter.sessionCreated(session);
	}

	@Override
	public void sessionIdle(NextFilter nextFilter, IoSession session, IdleStatus status) throws Exception {
		log(sessionIdleLevel, "IDLE {}", session.getRemoteAddress());
		nextFilter.sessionIdle(session, status);
	}

	@Override
	public void sessionOpened(NextFilter nextFilter, IoSession session) throws Exception {
		log(sessionOpenedLevel, "OPENED {}", session.getRemoteAddress());
		nextFilter.sessionOpened(session);
	}

	/**
	 * Set the LogLevel for the ExceptionCaught event.
	 * 
	 * @param level
	 *            The LogLevel to set
	 */
	public void setExceptionCaughtLogLevel(LogLevel level) {
		exceptionCaughtLevel = level;
	}

	/**
	 * Set the LogLevel for the MessageReceived event.
	 * 
	 * @param level
	 *            The LogLevel to set
	 */
	public void setMessageReceivedLogLevel(LogLevel level) {
		messageReceivedLevel = level;
	}

	/**
	 * Set the LogLevel for the MessageSent event.
	 * 
	 * @param level
	 *            The LogLevel to set
	 */
	public void setMessageSentLogLevel(LogLevel level) {
		messageSentLevel = level;
	}

	/**
	 * Set the LogLevel for the SessionClosed event.
	 * 
	 * @param level
	 *            The LogLevel to set
	 */
	public void setSessionClosedLogLevel(LogLevel level) {
		sessionClosedLevel = level;
	}

	/**
	 * Set the LogLevel for the SessionCreated event.
	 * 
	 * @param level
	 *            The LogLevel to set
	 */
	public void setSessionCreatedLogLevel(LogLevel level) {
		sessionCreatedLevel = level;
	}

	/**
	 * Set the LogLevel for the SessionIdle event.
	 * 
	 * @param level
	 *            The LogLevel to set
	 */
	public void setSessionIdleLogLevel(LogLevel level) {
		sessionIdleLevel = level;
	}

	/**
	 * Set the LogLevel for the SessionOpened event.
	 * 
	 * @param level
	 *            The LogLevel to set
	 */
	public void setSessionOpenedLogLevel(LogLevel level) {
		sessionOpenedLevel = level;
	}

	/**
	 * Log if the logger and the current event log level are compatible. We log
	 * a formated message and its parameters.
	 * 
	 * @param eventLevel
	 *            the event log level as requested by the user
	 * @param message
	 *            the formated message to log
	 * @param param
	 *            the parameter injected into the message
	 */
	private void log(LogLevel eventLevel, String message, Object... param) {
		switch (eventLevel) {
		case TRACE:
			logger.trace(message, param);
			return;
		case DEBUG:
			logger.debug(message, param);
			return;
		case INFO:
			logger.info(message, param);
			return;
		case WARN:
			logger.warn(message, param);
			return;
		case ERROR:
			logger.error(message, param);
			return;
		default:
			return;
		}
	}

	/**
	 * Log if the logger and the current event log level are compatible. We log
	 * a message and an exception.
	 * 
	 * @param eventLevel
	 *            the event log level as requested by the user
	 * @param message
	 *            the message to log
	 * @param cause
	 *            the exception cause to log
	 */
	private void log(LogLevel eventLevel, String message, Throwable cause) {
		switch (eventLevel) {
		case TRACE:
			logger.trace(message, cause);
			return;
		case DEBUG:
			logger.debug(message, cause);
			return;
		case INFO:
			logger.info(message, cause);
			return;
		case WARN:
			logger.warn(message, cause);
			return;
		case ERROR:
			logger.error(message, cause);
			return;
		default:
			return;
		}
	}
}