package com.memcache.response;

public enum ResponseStatus{
    STORED("STORED"),
    NOT_STORED("NOT_STORED"),
    NOT_FOUND("NOT_FOUND"),
    ERROR("ERROR"),
    END("END"),
    DELETED("DELETED");

    private String name;

    ResponseStatus(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}