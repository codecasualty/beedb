package com.memcache.response;
import com.memcache.cache.CacheItem;
import java.util.ArrayList;
import java.util.List;

public class Response {
    
    private ResponseStatus status;
    private List<CacheItem> items;

    public Response(ResponseStatus status){
        this.status = status;
        this.items = new ArrayList<>();
    }

    public ResponseStatus getStatus() {
        return status;
    }

    public List<CacheItem> getItems() {
        return items;
    }

    public void addItem(CacheItem item) {
        items.add(item);
    }

    public String toProtocolString() {
        StringBuilder sb = new StringBuilder();
        for (CacheItem item : items) {
            sb.append("VALUE ");
            sb.append(item.getKey()).append(" ");
            sb.append(item.getFlags()).append(" ");
            sb.append(item.getValue().length).append("\r\n");
            sb.append(new String(item.getValue())).append("\r\n");
        }
        sb.append(status.name()).append("\r\n");
        return sb.toString();
    }
}
