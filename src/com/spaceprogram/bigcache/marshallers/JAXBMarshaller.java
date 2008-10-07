package com.spaceprogram.bigcache.marshallers;

import com.spaceprogram.bigcache.S3Cache;
import org.jets3t.service.model.S3Object;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.namespace.QName;
import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Uses JAXB.
 *
 * User: treeder
 * Date: Sep 22, 2008
 * Time: 6:33:31 PM
 */
public class JAXBMarshaller implements Marshaller {

    private Map<Class, JAXBContext> contexts = new ConcurrentHashMap<Class, JAXBContext>();

    public JAXBMarshaller() {
    }

    public byte[] marshal(Serializable object) throws Exception {
        JAXBContext context = getContext(object.getClass());
        javax.xml.bind.Marshaller marshaller = context.createMarshaller();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // now XMLRootElement annotation required
        marshaller.marshal(new JAXBElement(new QName(object.getClass().getSimpleName()), object.getClass(), object), out);
//        marshaller.marshal(object, out);
        return out.toByteArray();
    }

    private JAXBContext getContext(Class c) throws JAXBException {
        JAXBContext context = contexts.get(c);
        if (context == null) {
            context = JAXBContext.newInstance(c);
            contexts.put(c, context);
        }
        return context;
    }

    public Object unmarshal(InputStream inputStream, S3Object s3object) throws Exception {
        // for JAXBMarshaller Class parameter, we could store the class in the object's meta-data when pushing it out.
        String className = (String) s3object.getMetadata(S3Cache.CLASS_META_NAME);
        if(className == null){
            throw new JAXBException("No class type found in S3 metadata. Was this object PUT via BigCache using the JAXBMarshaller?");
        }
        Class expectedType = Class.forName(className);

        JAXBContext context = getContext(expectedType);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        JAXBElement o = unmarshaller.unmarshal(new StreamSource(inputStream), expectedType);
//        Object o = unmarshaller.unmarshal(inputStream);
        return o.getValue();
    }

    public void addHeaders(S3Object s3o, Serializable object) {
        s3o.addMetadata(S3Cache.CLASS_META_NAME, object.getClass().getName()); // for unmarshalling
    }
}
