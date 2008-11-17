package com.spaceprogram.bigcache;

import com.spaceprogram.bigcache.marshallers.JAXBMarshaller;
import com.spaceprogram.bigcache.marshallers.Marshaller;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Cache implementation for S3.
 * <p/>
 * User: treeder
 * Date: Aug 28, 2008
 * Time: 9:27:53 AM
 */
public class S3Cache implements BigCache {
    private String awsAccessKey;
    private String awsSecretKey;
    private String bucketName;
    private S3Service s3Service;
    private S3Bucket bucket;
    public static String EXPIRES_META_NAME = "cache-expires";
    public static final String CLASS_META_NAME = "class-name";
    private AtomicStatistics statistics = new AtomicStatistics();
    private static Logger logger = Logger.getLogger(S3Cache.class.getName());
    private Marshaller marshaller = new JAXBMarshaller();
    private ExecutorService executorService;

    /**
     * @param awsAccessKey
     * @param awsSecretKey
     * @param bucketName   the name of the S3 bucket you'd like to store your cache objects in
     */
    public S3Cache(String awsAccessKey, String awsSecretKey, String bucketName) {
        this.awsAccessKey = awsAccessKey;
        this.awsSecretKey = awsSecretKey;
        this.bucketName = bucketName;

        try {
            AWSCredentials awsCredentials = new AWSCredentials(awsAccessKey, awsSecretKey);
            s3Service = new RestS3Service(awsCredentials);
            bucket = s3Service.createBucket(bucketName);
            System.out.println("S3Cache: Created caching bucket: " + bucket.getName());
        } catch (S3ServiceException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param awsAccessKey
     * @param awsSecretKey
     * @param bucketName      the name of the S3 bucket you'd like to store your cache objects in
     * @param executorService must pass this in if you plan to use asynchronous methods.
     */
    public S3Cache(String awsAccessKey, String awsSecretKey, String bucketName, ExecutorService executorService) {
        this.awsAccessKey = awsAccessKey;
        this.awsSecretKey = awsSecretKey;
        this.bucketName = bucketName;
        this.executorService = executorService;

        try {
            AWSCredentials awsCredentials = new AWSCredentials(awsAccessKey, awsSecretKey);
            s3Service = new RestS3Service(awsCredentials);
            bucket = s3Service.createBucket(bucketName);
            System.out.println("S3Cache: Created caching bucket: " + bucket.getName());
        } catch (S3ServiceException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        long x = System.currentTimeMillis() + ((long) Integer.MAX_VALUE * 1000L);
        System.out.println("x=" + x);
        System.out.println("y=" + (Long.MAX_VALUE - ((long) Integer.MAX_VALUE * 1000L)));
    }

    /**
     *
     * @param key
     * @param object
     * @param expiresInSeconds
     * @throws Exception
     */
    public void put(String key, Serializable object, int expiresInSeconds) throws Exception {
        try {
            S3Object s3o = new S3Object(key);
            s3o.addMetadata(EXPIRES_META_NAME, Long.toString(System.currentTimeMillis() + ((long) expiresInSeconds * 1000L)));
            marshaller.addHeaders(s3o, object);
            byte[] byteArray = marshaller.marshal(object);
//            System.out.println("writing: " + new String(byteArray));
            s3o.setContentLength(byteArray.length);
            ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
            s3o.setDataInputStream(bais);
            statistics.puts.incrementAndGet();
            object = s3Service.putObject(bucket, s3o);
            bais.close(); // no effect, but just for good practice
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Puts an object to the cache in the background. Returns immediately.
     *
     * @param key
     * @param object
     * @param expiresInSeconds
     * @return
     */
    public Future<Object> putAsync(String key, Serializable object, int expiresInSeconds){
         checkExecutor();
        Put put = new Put(this, key, object, expiresInSeconds);
        return executorService.submit(put);
    }

    /**
     * Just because memcached has it: adds to the cache, only if it doesn't already exist (get_foo() should use this)
     * @param key
     * @param object
     * @param expiresInSeconds
     * @throws Exception
     */
    public void add(String key, Serializable object, int expiresInSeconds) throws Exception {
        // user getObjectDetails to see if it exists
        try {
            S3Object s3Object = s3Service.getObjectDetails(bucket, key);
            if (s3Object == null || expired(s3Object)) {
                put(key, object, expiresInSeconds);
            }
        } catch (S3ServiceException e) {
            throw e;
        }
    }

    /**
     * Just because memcached has it: sets in the cache only if the key already exists (not as useful, only for completeness)
     * @param key
     * @param object
     * @param expiresInSeconds
     * @throws Exception
     */
    public void replace(String key, Serializable object, int expiresInSeconds) throws Exception {
        try {
            S3Object s3Object = s3Service.getObjectDetails(bucket, key);
            if (s3Object != null) {
                if (!expired(s3Object)) {
                    put(key, object, expiresInSeconds);
                }
            }
        } catch (S3ServiceException e) {
            throw e;
        }
    }

    private boolean expired(S3Object s3Object) {
        String expiryAsString = (String) s3Object.getMetadata(EXPIRES_META_NAME);
//        System.out.println("expiryAsString=" + expiryAsString);
        if (expiryAsString == null) {
            // don't allow this
            return true;
        }
        long expires = Long.parseLong(expiryAsString);
        if (expires < System.currentTimeMillis()) {
            logger.fine("Object expired.  expired-header=" + expires);
            return true;
        }
        return false;
    }

    /**
     * Gets an object from the cache in the background. Useful in various scenarios, but one in
     * particular is for getting objects in a webapp before rendering the view. When rendering the view
     * you call get() on the future that is returned from this method.
     *
     * @param key
     * @return
     */
    public Future<Serializable> getAsync(String key) {
        checkExecutor();
        Get get = new Get(this, key);
        return executorService.submit(get);
    }

    private void checkExecutor() {
        if(executorService == null){
            throw new BigCacheRuntimeException("ExecutorService is null in S3Cache. Can not perform operation.");
        }
    }

    public Serializable get(String key) throws Exception {
        S3Object s3Object = null;
        try {
//            System.out.println("getting object from s3 with key: " + key);
            s3Object = s3Service.getObject(bucket, key);
//            System.out.println("Got " + s3Object);
            statistics.gets.incrementAndGet();
            if (s3Object == null) return null;
            if (logger.isLoggable(Level.FINEST)) {
//                Spits out headers
                Map<String, Object> metaMap = s3Object.getMetadataMap();
                for (Map.Entry<String, Object> stringStringEntry : metaMap.entrySet()) {
                    System.out.println(stringStringEntry.getKey() + " - " + stringStringEntry.getValue());
                }
            }
            if (expired(s3Object)) {
                logger.fine(("object expired, removing. key=" + key));
//                System.out.println("object expired, removing. key=" + key);
                s3Object.closeDataInputStream();
                // todo: deal with this remove some other way, too slow
//                remove(key);
                return null;
            }
            statistics.hits.incrementAndGet();
            Object o = marshaller.unmarshal(s3Object.getDataInputStream(), s3Object);
            s3Object.closeDataInputStream();
            return (Serializable) o;
        } catch (S3ServiceException e) {
            if (e.getS3ErrorCode().equals("NoSuchKey")) {
                logger.fine("NoSuchKey: " + key);
//                System.out.println("no such key");
                // doesn't exist which is fine
            } else {
                throw e;
            }
        } catch (InvalidClassException e) {
            // Serialization breakage
//            System.out.println("WARNING InvalidClassException [key=" + key + "]: " + e.getMessage());
            throw e;
        } catch (ClassNotFoundException e) {
            throw e;
        } finally {
            if (s3Object != null) {
                try {
                    s3Object.closeDataInputStream();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    public void remove(String key) throws Exception {
        try {
            s3Service.deleteObject(bucket, key);
            statistics.removes.incrementAndGet();
        } catch (S3ServiceException e) {
            throw e;
        }
    }

    public AtomicStatistics getStatistics() {
        return statistics;
    }

    public void setMarshaller(JAXBMarshaller marshaller) {
        this.marshaller = marshaller;
    }


    public ExecutorService getExecutorService() {
        return executorService;
    }
}
