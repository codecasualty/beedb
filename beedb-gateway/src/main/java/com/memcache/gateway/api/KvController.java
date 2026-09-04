package com.memcache.gateway.api;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.memcache.gateway.client.BeedbClient;

@RestController
@RequestMapping("/api/kv")
public class KvController {
    
    private final BeedbClient beedbClient;

    public KvController(BeedbClient beedbClient){
        this.beedbClient = beedbClient;
    }

    @GetMapping ("/{key}")
    public ResponseEntity<String> get(@PathVariable("key") String key){
        String value = beedbClient.get(key);
        if(value == null){
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(value);
    }
   @PutMapping (value="/{key}", consumes = MediaType.ALL_VALUE)
    public ResponseEntity<String> set(@PathVariable("key") String key, @RequestBody String value){
        String response = beedbClient.set(key, value);
        if(response == null || response.equals("NOT_STORED")){
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(response);
    }

    @DeleteMapping ("/{key}")
    public ResponseEntity<String> delete(@PathVariable("key") String key){
        String response = beedbClient.delete(key);
        if(response == null || response.equals("NOT_FOUND")){
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(response);
    }
}
