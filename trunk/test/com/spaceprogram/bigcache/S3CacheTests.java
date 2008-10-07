package com.spaceprogram.bigcache;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

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
public class S3CacheTests {
    static S3Cache s3cache;

    @BeforeClass
    public static void setupCache() throws IOException {
        System.out.println("Setting up cache in S3CacheTests");
        Properties props = new Properties();
        InputStream is = S3CacheTests.class.getResourceAsStream("/aws-auth.properties");
        if(is == null){
            throw new RuntimeException("No aws-auth.properties file found.");
        }
        props.load(is);
        s3cache = new S3Cache(props.getProperty("accessKey"), props.getProperty("secretKey"), props.getProperty("bucketName"));
    }

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
}
