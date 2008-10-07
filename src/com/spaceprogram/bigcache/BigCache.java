package com.spaceprogram.bigcache;

import org.jets3t.service.S3ServiceException;

import java.io.Serializable;
import java.io.IOException;

/**
 * Main cache interface.
 *
 * User: treeder
 * Date: Aug 28, 2008
 * Time: 9:22:30 AM
 */
public interface BigCache {

    /**
     * Unconditional set.
     * @param key
     * @param object
     * @param expiryTimeInSeconds
     * @throws S3ServiceException
     * @throws IOException
     */
    void put(String key, Serializable object, int expiryTimeInSeconds) throws Exception;

    /**
     * Adds to cache only if it doesn't exist.
     * @param key
     * @param object
     * @param expiryTimeInSeconds
     */
    void add(String key, Serializable object, int expiryTimeInSeconds) throws Exception;

    /**
     * Sets in cache only if it does exist.
     * @param key
     * @param object
     * @param expiryTimeInSeconds
     */
    void replace(String key, Serializable object, int expiryTimeInSeconds) throws Exception;

    /**
     * Gets the object from the cache.
     * @param key
     */
    Serializable get(String key) throws Exception;

    /**
     * Removes the object from the cache.
     * @param key
     */
    void remove(String key) throws Exception;
}
