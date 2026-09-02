package com.memcache.gateway.client.beedbexception;

/**
 * Base type for every failure to reach or use the BeeDB cluster.
 * A checked exception would put {@code throws} on every method between the socket
 * and the controller, and none of those frames can do anything about it. Unchecked
 * lets the failure travel to the one place that can decide -- the HTTP layer --
 * without ceremony in between.
 * Both Java's {@code catch} and Spring's {@code @ExceptionHandler} dispatch on
 * TYPE. Because each failure maps to a different HTTP status, making them separate
 * types lets the language do the routing instead of a switch statement we maintain
 * by hand. The base class is what gives us the coarse option as well -- a caller
 * that does not care which failure it was can still catch BeedbException and get
 * all of them.
 * The (message, cause) constructor is the important one. wrapping a
 * ConnectException without its cause throws away the address
 */
public class BeedbException extends RuntimeException {

    public BeedbException(String message) {
        super(message);
    }

    /**
     * Prefer this at every wrapping site:
     *
     *   catch (ConnectException e) {
     *       throw new NodeUnreachableException("node " + address + " unreachable", e);
     *   }
     *
     * A logged trace then shows "Caused by: java.net.ConnectException: Connection
     * refused" underneath ours, which is the part that says what actually happened.
     */
    public BeedbException(String message, Throwable cause) {
        super(message, cause);
    }
}
