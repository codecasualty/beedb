package com.memcache.gateway.client;
import java.util.*; import java.util.concurrent.*; import java.util.concurrent.atomic.*;
public class ConcProbe {
    public static void main(String[] a) throws Exception {
        int threads  = Integer.parseInt(System.getProperty("threads", "64"));
        int seconds  = Integer.parseInt(System.getProperty("seconds", "30"));
        Map<String,String> nodes = new LinkedHashMap<>();
        nodes.put("node1","127.0.0.1:11211");
        nodes.put("node2","127.0.0.1:11212");
        nodes.put("node3","127.0.0.1:11213");
        BeedbClient c = new BeedbClient(nodes, 86400);
        c.getleaderAddressString();

        // warm up: JIT + pool fill, not measured
        ExecutorService warm = Executors.newFixedThreadPool(threads);
        CountDownLatch wl = new CountDownLatch(threads);
        for (int t=0;t<threads;t++){ final int id=t; warm.submit(()->{ try{ for(int i=0;i<20;i++) c.set("w"+id+"_"+i,"x"); } finally { wl.countDown(); } }); }
        wl.await(60, TimeUnit.SECONDS); warm.shutdownNow();

        AtomicLong ok=new AtomicLong(), bad=new AtomicLong();
        AtomicBoolean stop=new AtomicBoolean();
        ExecutorService ex = Executors.newFixedThreadPool(threads);
        long t0=System.currentTimeMillis();
        for (int t=0;t<threads;t++){
            final int id=t;
            ex.submit(()->{ int i=0; while(!stop.get()){
                if("STORED".equals(c.set("p"+id+"_"+(i++), "value-32-bytes-padding-xxxxxxxx"))) ok.incrementAndGet(); else bad.incrementAndGet(); }});
        }
        Thread.sleep(seconds*1000L);
        stop.set(true); ex.shutdown(); ex.awaitTermination(30, TimeUnit.SECONDS);
        long ms=System.currentTimeMillis()-t0;
        System.out.printf("%d threads, %ds : %d writes = %d writes/sec  (errors %d)%n",
            threads, seconds, ok.get(), 1000*ok.get()/ms, bad.get());
    }
}
