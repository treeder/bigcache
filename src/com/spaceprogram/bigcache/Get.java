package com.spaceprogram.bigcache;

import java.util.concurrent.Callable;

/**
 * User: treeder
 * Date: Nov 14, 2008
 * Time: 10:25:35 AM
 */
public class Get implements Callable {
    private S3Cache cache;
    private String key;

    public Get(S3Cache cache, String key) {
        this.cache = cache;
        this.key = key;
    }

    public Object call() throws Exception {
        return cache.get(key);
    }
}
