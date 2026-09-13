package com.memcache.gateway.api;

import java.util.Map;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.memcache.gateway.kv.WriteRateLimiter;

/**
 * How many writes this client has left this hour, so the page shows the server's real count
 * instead of guessing
 */
@RestController
@RequestMapping("/api")
public class QuotaController {

    private final WriteRateLimiter rateLimiter;

    public QuotaController(WriteRateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    @GetMapping("/quota")
    public Map<String, Integer> quota(HttpServletRequest request) {
        String client = request.getRemoteAddr();
        return Map.of("remaining", rateLimiter.remaining(client),
                      "limit", rateLimiter.maxWritesPerHour());
    }
}
