package com.memcache.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;

public final class FailStop {

    private FailStop() { }

    public static void halt(Logger logger, String what, Throwable cause) {
        try {
            logger.error("FAIL-STOP: {} -- halting this node", what, cause);
            System.err.println("FAIL-STOP: " + what + ": " + cause);
            cause.printStackTrace();
            if (LoggerFactory.getILoggerFactory() instanceof LoggerContext context) {
                // drains the AsyncAppender queue, which halt() would drop
                context.stop();   
            }
        } finally {
            Runtime.getRuntime().halt(1);
        }
    }
}
