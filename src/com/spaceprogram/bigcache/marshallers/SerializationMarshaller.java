package com.spaceprogram.bigcache.marshallers;

import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Object;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Uses standard java serialization.
 *
 * User: treeder
 * Date: Sep 22, 2008
 * Time: 6:29:10 PM
 */
public class SerializationMarshaller implements Marshaller {
    public byte[] marshal(Serializable object) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);
        out.writeObject(object);
        out.close();
        byte[] byteArray = bos.toByteArray();
        return byteArray;
    }

    public Object unmarshal(InputStream inputStream, S3Object s3object) throws IOException, S3ServiceException, ClassNotFoundException {
        ObjectInputStream in = new ObjectInputStream(inputStream);
        Object o = in.readObject();
        in.close();
        return o;
    }

    public void addHeaders(S3Object s3o, Serializable object) {

    }
}
