package com.socialbakers.phoenix.proxy.server;

import org.apache.commons.lang.StringUtils;

/**
 * @author robert
 */
public class Logger {
    
    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ProxyServer.class.getName());
    
    private static long errCounter;
    
    static void debug(String msg) {
        logger.debug(msg);
    }
    
    static void error(Throwable t) {
        error(null, t);
    }
    
    static void error(String msg, Throwable t) {
        errCounter++;
        if (StringUtils.isBlank(msg) && t != null) {
            msg = t.getMessage();
        }
        logger.error(msg, t);
    }
    
    static void resetErrCounter() {
        errCounter = 0;
    }
    
    static long getErrCount() {
        return errCounter;
    }
}
