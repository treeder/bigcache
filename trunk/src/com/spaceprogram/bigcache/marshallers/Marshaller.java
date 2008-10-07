package com.spaceprogram.bigcache.marshallers;

import org.jets3t.service.model.S3Object;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;

/**
 * User: treeder
 * Date: Sep 22, 2008
 * Time: 6:28:36 PM
 */
public interface Marshaller {
    byte[] marshal(Serializable object) throws Exception;

    Object unmarshal(InputStream inputStream, S3Object s3object) throws Exception;

    void addHeaders(S3Object s3o, Serializable object);

}
