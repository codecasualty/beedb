package com.memcache.gateway.demo;

public record DemoRecord (
    long ok, long failed , long unconfirmed , String lastKey
){

}
