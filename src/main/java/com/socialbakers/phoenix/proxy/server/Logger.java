/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.socialbakers.phoenix.proxy.server;

import org.apache.commons.lang.StringUtils;
//import org.apache.log4j.BasicConfigurator;

/**
 * @author robert
 */
public class Logger {
    
    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ProxyServer.class.getName());
    
//    static {
//        BasicConfigurator.configure();
//    }

//    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProxyServer.class.getName());
    
    static void debug(String msg) {
        logger.debug(msg);
    }
    
    static void error(Throwable t) {
        error(null, t);
    }
    
    static void error(String msg, Throwable t) {
        if (StringUtils.isBlank(msg) && t != null) {
            msg = t.getMessage();
        }
        logger.error(msg, t);
    }
}
