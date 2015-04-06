package de.tuberlin.pserver.utils;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.SerializationUtils;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.Serializable;


public interface ObjectSerializer {

    public abstract byte[] serialize(final Object obj);

    public abstract <T> T deserialize(final byte[] data);

    // ---------------------------------------------------

    public static enum SerializerType {

        JAVA_SERIALIZER,

        KRYO_SERIALIZER;
    }

    // ---------------------------------------------------

    public static final class Factory {

        private Factory() {}

        public static ObjectSerializer create(final SerializerType type) {
            switch (Preconditions.checkNotNull(type)) {
                case JAVA_SERIALIZER: new JavaObjectSerializer();
                case KRYO_SERIALIZER:
                    throw new NotImplementedException();
                default:
                    throw new IllegalStateException();
            }
        }
    }

    // ---------------------------------------------------

    static final class JavaObjectSerializer implements ObjectSerializer {

        @Override
        public byte[] serialize(final Object obj) {
            return SerializationUtils.serialize((Serializable) Preconditions.checkNotNull(obj));
        }

        @Override
        public <T> T deserialize(final byte[] data) {
            return SerializationUtils.deserialize(Preconditions.checkNotNull(data));
        }
    }
}
