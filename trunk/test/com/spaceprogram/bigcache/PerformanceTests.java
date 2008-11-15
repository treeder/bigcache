package com.spaceprogram.bigcache;

import org.apache.commons.lang.time.StopWatch;
import org.junit.Test;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.io.Serializable;

/**
 * User: treeder
 * Date: Nov 14, 2008
 * Time: 5:32:12 PM
 */
public class PerformanceTests extends BaseCacheTest {

    @Test
    public void testPuts() throws Exception {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        for (int i = 0; i < 100; i++) {
            String id = "id-" + i;
            String s = i + " - bigcache is rad.";
            s3cache.put(id, s, 3600);
        }
        stopWatch.stop();
        System.out.println("duration = " + stopWatch.getTime());
    }

    @Test
    public void testPutsAsync() throws Exception {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < 100; i++) {
            String id = "id-" + i;
            String s = i + " - bigcache is rad.";
            Future f = s3cache.putAsync(id, s, 3600);
            futures.add(f);
        }
        // make sure all objects are done putting
        for (Future future : futures) {
            future.get();
        }
        stopWatch.stop();
        System.out.println("duration = " + stopWatch.getTime());
    }

    @Test
    public void testGets() throws Exception {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        for (int i = 0; i < 100; i++) {
            String id = "id-" + i;
            Serializable serializable = s3cache.get(id);
        }
        stopWatch.stop();
        System.out.println("duration = " + stopWatch.getTime());
    }

     @Test
    public void testGetsAsync() throws Exception {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
         List<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < 100; i++) {
            String id = "id-" + i;
            Future<Serializable> f = s3cache.getAsync(id);
            futures.add(f);
        }
          // make sure all objects are done getting
        for (Future future : futures) {
            Object o = future.get();
//            System.out.println("got: " + o);
        }
        stopWatch.stop();
        System.out.println("duration = " + stopWatch.getTime());
    }
}
