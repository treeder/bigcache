package com.spaceprogram.bigcache;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Put a file in the root classpath called aws-auth.properties with three lines:
 * accessKey = ABC
 * secretKey = DEF
 * bucketName = test bucket name
 *
 * User: treeder
 * Date: Aug 28, 2008
 * Time: 10:31:19 AM
 */
public class S3CacheTests extends BaseCacheTest {

    @Test
    public void testSet() throws Exception {
        String key = "x";
        String s = "This is my data for the cache.";
        s3cache.put(key, s, 3600);
        String ret = (String) s3cache.get(key);
        Assert.assertEquals(s, ret);
    }

    @Test
    public void testGetNotExists() throws Exception {
        String key = "this-key-does-not-exist";
        String ret = (String) s3cache.get(key);
        Assert.assertNull(ret);
    }

    @Test
    public void testRemoveNotExists() throws Exception {
        String key = "this-key-does-not-exist";
        s3cache.remove(key);
    }

    @Test
    public void testExpired() throws Exception {
        String key = "shortExpiry";
        String s = "This is my data for the cache.";
        s3cache.put(key, s, 1);
        Thread.sleep(2000);
        String ret = (String) s3cache.get(key);
        Assert.assertNull(ret);
    }

    @Test
    public void testGetBunch() throws Exception {
          for(int i = 0; i < 50; i++){
            SomeObject2 someObject2 = new SomeObject2("name" + i);
            s3cache.put("key" + i, someObject2, 3600);
        }
        long start = System.currentTimeMillis();
        for(int i = 0; i < 50; i++) {
            Object o = s3cache.get("key" + i);
            System.out.println(o);
        }
         System.out.println("Duration: " + (System.currentTimeMillis() - start));
    }

    @Test
    public void testGetAsync() throws Exception {
        for(int i = 0; i < 50; i++){
            SomeObject2 someObject2 = new SomeObject2("name" + i);
            s3cache.put("key" + i, someObject2, 3600);
        }
        long start = System.currentTimeMillis();
        List<Future> fromCache = new ArrayList<Future>();
        for(int i = 0; i < 50; i++){
            fromCache.add(s3cache.getAsync("key" + i));
        }
        for (Future future : fromCache) {
            Object o = future.get();
            System.out.println(o);
        }
        System.out.println("Duration: " + (System.currentTimeMillis() - start));
    }
}
