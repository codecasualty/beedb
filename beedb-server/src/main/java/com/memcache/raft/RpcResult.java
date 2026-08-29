package com.memcache.raft;

public class RpcResult<T> {
    /*
    * OK :- success, got the response as non-null, caller must process it
    * TIMEOUT :- timeout occured while waiting for response , retry next tick dont backoff
    * UNREACHABLE :- unreachable peer, backoff 
    * BAD_RESPONSE :- response was not as expected
     */
    public enum Status {
        OK,
        TIMEOUT,
        UNREACHABLE,
        BAD_RESPONSE
    }

    private final T response;
    private final Status status;
    private final String exceptionName;

    private RpcResult(T response, Status status, String exceptionName) {
        this.response = response;
        this.status = status;
        this.exceptionName = exceptionName;
    }

    public static <T> RpcResult<T> ok(T response) {
        if(response == null) throw new IllegalArgumentException("response cannot be null");
        return new RpcResult<>(response, Status.OK, null);
    }

    public static <T> RpcResult<T> timeout(String exceptionName) {
        return new RpcResult<>(null, Status.TIMEOUT, exceptionName);
    }

    public static <T> RpcResult<T> unreachable(String exceptionName) {
        return new RpcResult<>(null, Status.UNREACHABLE, exceptionName);
    }

    public static <T> RpcResult<T> badResponse(String exceptionName) {
        return new RpcResult<>(null, Status.BAD_RESPONSE, exceptionName);
    }

    public T getResponse() {
        return response;
    }

    public Status getStatus() {
        return status;
    }

    public String getExceptionName() {
        return exceptionName;
    }

    public boolean isOk() {
        return status == Status.OK;
    }

    public boolean isTimeout() {
        return status == Status.TIMEOUT;
    }

    public boolean isUnreachable() {
        return status == Status.UNREACHABLE;
    }

    public boolean isBadResponse() {
        return status == Status.BAD_RESPONSE;
    }
}
