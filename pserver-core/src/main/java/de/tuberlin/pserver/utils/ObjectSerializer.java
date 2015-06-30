package de.tuberlin.pserver.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.SerializationUtils;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;


public interface ObjectSerializer {

    public abstract byte[] serialize(final Object obj);

    public abstract <T> T deserialize(final byte[] data);

    public abstract <T> T deserialize(final byte[] data, final Class<T> clazz);

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
                case JAVA_SERIALIZER: return new JavaObjectSerializer();
                case KRYO_SERIALIZER: return new KryoObjectSerializer();
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

        @Override
        public <T> T deserialize(byte[] data, Class<T> clazz) {
            return SerializationUtils.deserialize(Preconditions.checkNotNull(data));
        }
    }

    // ---------------------------------------------------

    static final class KryoObjectSerializer implements ObjectSerializer {

        private Kryo kryo = new Kryo();

        @Override
        public byte[] serialize(Object obj) {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            Output output = new UnsafeOutput(stream);
            kryo.writeObject(output, obj);
            output.close();
            final byte[] data = stream.toByteArray();
            return data;
        }

        @Override
        public <T> T deserialize(byte[] data) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T deserialize(byte[] data, final Class<T> clazz) {
            ByteArrayInputStream stream = new ByteArrayInputStream(data);
            Input input = new UnsafeInput(stream);
            T obj = kryo.readObject(input, clazz);
            input.close();
            return obj;
        }
    }
}
