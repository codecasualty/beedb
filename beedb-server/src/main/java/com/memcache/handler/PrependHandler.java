package com.memcache.handler;

import com.memcache.cache.Cache;
import com.memcache.command.Command;
import com.memcache.response.Response;
import com.memcache.response.ResponseStatus;
public class PrependHandler implements CommandHandler{

    @Override
    public Response execute(Command command, Cache cache) {
        String keyString = command.getKey();
        if(cache.containsKey(keyString)){
            // here new STring() is not binary safe, means, it will use platform default encoding and may corrupt the data
            // String value = new String(command.getValue()) + new String(cache.get(keyString).getValue());
            // cache.put(keyString, value.getBytes(StandardCharsets.UTF_8), command.getFlags(), command.getExpiry());
            byte[] existingValue = cache.get(keyString).getValue();
            byte [] appendedValue = command.getValue();
            byte[] combinedValue = new byte[existingValue.length + appendedValue.length];
            System.arraycopy(appendedValue, 0, combinedValue, 0, appendedValue.length);
            System.arraycopy(existingValue, 0, combinedValue, appendedValue.length, existingValue.length);
            cache.put(keyString, combinedValue, command.getFlags(), command.getExpiry());
            // public static void arraycopy(Object src, int srcPos, Object dest, int destPos, int length)
            return new Response(ResponseStatus.STORED);
        }
        return new Response(ResponseStatus.NOT_STORED);
    }
    
}
