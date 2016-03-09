package de.tuberlin.pserver.benchmarks.criteo.logreg;


import de.tuberlin.pserver.commons.serialization.ObjectSerializer;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public final class LocalFSObjectStorage {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final ObjectSerializer serializer
            = ObjectSerializer.Factory.create(ObjectSerializer.SerializerType.KRYO_SERIALIZER);

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    private LocalFSObjectStorage() {}

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static void writeTo(Object obj, String path) {
        byte[] data = serializer.serialize(obj);
        try {
            FileUtils.writeByteArrayToFile(new File(path), data);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Object readFrom(String path, Class<?> type) {
        try {
            byte[] data = FileUtils.readFileToByteArray(new File(path));
            return serializer.deserialize(data, type);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
