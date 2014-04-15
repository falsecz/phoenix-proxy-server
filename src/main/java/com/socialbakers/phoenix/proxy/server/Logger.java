/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.socialbakers.phoenix.proxy.server;

import java.util.logging.Level;
import org.apache.commons.lang.StringUtils;

/**
 * @author robert
 */
public class Logger {
    
    private static final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(ProxyServer.class.getName());
    
    static {
//        CustomRecordFormatter formatter = new Cus
    }
    
    static void log(String msg) {
        logger.log(Level.SEVERE, msg);
    }

    static void log(Throwable t) {
        log(null, t);
    }
    
    static void log(String msg, Throwable t) {
        if (StringUtils.isBlank(msg) && t != null) {
            msg = t.getMessage();
        }
        logger.log(Level.SEVERE, msg, t);
    }
}
