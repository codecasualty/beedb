package com.memcache.handler;


import com.memcache.cache.Cache;
import com.memcache.command.Command;
import com.memcache.response.Response;
import com.memcache.response.ResponseStatus;
public class AppendHandler implements CommandHandler{

    @Override
    public Response execute(Command command, Cache cache){
        String keyString = command.getKey();
        if(cache.containsKey(keyString)){
            // here new STring() is not binary safe, means, it will use platform default encoding and may corrupt the data
            // String value = new String(cache.get(keyString).getValue()) + new String(command.getValue());
            // cache.put(keyString, value.getBytes(StandardCharsets.UTF_8), command.getFlags(), command.getExpiry());
            byte[] existingValue = cache.get(keyString).getValue();
            byte [] appendedValue = command.getValue();
            byte[] combinedValue = new byte[existingValue.length + appendedValue.length];

            // public static void arraycopy(Object src, int srcPos, Object dest, int destPos, int length)
            System.arraycopy(existingValue, 0, combinedValue, 0 , existingValue.length);
            System.arraycopy(appendedValue, 0, combinedValue, existingValue.length, appendedValue.length);
            cache.put(keyString, combinedValue, command.getFlags(), command.getExpiry());

            return new Response(ResponseStatus.STORED);
        }
        return new Response(ResponseStatus.NOT_STORED);
    }
    
}
