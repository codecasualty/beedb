package com.memcache.gateway.client;

import java.io.IOException;

/**
 * One command sent on one already-borrowed connection.
 *
 * This exists so that get / set / delete can share everything EXCEPT the single
 * line that differs between them:
 *
 *     get     ->  connection -> connection.getValue(key)
 *     set     ->  connection -> connection.setValue(key, value)
 *     delete  ->  connection -> connection.deleteValue(key)
 * Function.apply cannot throw a checked exception, and every BeedbConnection
 * method declares IOException. Wrapping each lambda in its own try/catch to
 * satisfy Function would put back the duplication this is here to remove. A
 * one-method interface that declares IOException costs four lines and lets the
 * lambdas stay a single expression.
 *
 * @FunctionalInterface is not required for lambdas to work -- it is a compile-time
 * assertion that this interface has exactly one abstract method, so that adding a
 * second one later fails here rather than at every call site.
 */
@FunctionalInterface
public interface BeedbOperation {

    /**
     * Run the command. The connection is borrowed and owned by the caller --
     * do not close it, pool it, or keep a reference to it.
     *
     * @return the server's reply line, or null for a cache miss
     */
    String apply(BeedbConnection connection) throws IOException;
}
