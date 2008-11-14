package com.spaceprogram.bigcache;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * User: treeder
 * Date: Nov 14, 2008
 * Time: 10:21:01 AM
 */
public class Put implements Callable {
    private static Logger logger = Logger.getLogger(Put.class.getName());
    private S3Cache cache;
    private String key;
    private Serializable object;
    private int expiryTimeInSeconds;

    public Put(S3Cache cache, String key, Serializable object, int expiryTimeInSeconds) {
        this.cache = cache;
        this.key = key;
        this.object = object;
        this.expiryTimeInSeconds = expiryTimeInSeconds;
    }

    public Object call() throws Exception {
        logger.fine("DelayedPut running [key=" + key + "], putting " + object);
        try {
            // put below removes the object from the delayMap
            cache.put(key, object, expiryTimeInSeconds);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error on Put operation: " + e.getMessage(), e);
            throw e;
        }
        return object;
    }
}
