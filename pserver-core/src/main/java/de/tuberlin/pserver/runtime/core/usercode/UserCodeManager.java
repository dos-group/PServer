package de.tuberlin.pserver.runtime.core.usercode;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.compression.Compressor;
import de.tuberlin.pserver.math.tuples.Tuple2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public final class UserCodeManager {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private static final class DynamicClassLoader extends ClassLoader {

        final Map<String, Class<?>> loadedClazzMap;

        public DynamicClassLoader(final ClassLoader cl) {
            super(cl);
            this.loadedClazzMap = new HashMap<>();
        }

        public Class<?> buildClassFromByteArray(final String clazzName, final byte[] clazzData) {
            Preconditions.checkNotNull(clazzName);
            Preconditions.checkNotNull(clazzData);
            if (!loadedClazzMap.containsKey(clazzName)) {
                final Class<?> newClazz;
                try {
                    newClazz = this.defineClass(clazzName, clazzData, 0, clazzData.length);
                } catch (ClassFormatError e) {
                    throw new IllegalStateException(e);
                }
                loadedClazzMap.put(clazzName, newClazz);
                return newClazz;
            } else
                return loadedClazzMap.get(clazzName);
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final DynamicClassLoader classLoader;

    private final Compressor compressor = Compressor.Factory.create(Compressor.CompressionType.JAVA_COMPRESSION);

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public UserCodeManager(final ClassLoader cl) {
        this.classLoader = new DynamicClassLoader(Preconditions.checkNotNull(cl));
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public List<Tuple2<String, byte[]>> extractClass(final Class<?> clazz) { return extractClass(clazz, new LinkedList<>()); }
    @SuppressWarnings("unchecked")
    public List<Tuple2<String, byte[]>> extractClass(final Class<?> clazz, final List<Tuple2<String, byte[]>> clazzesBC) {
        Preconditions.checkNotNull(clazz);
        for (final Class<?> declaredClazz : clazz.getDeclaredClasses()) {
            extractClass(declaredClazz, clazzesBC);
        }
        clazzesBC.add(new Tuple2(clazz.getName(), compressor.compress(readByteCode(clazz))));
        return clazzesBC;
    }

    public Class<?> implantClass(final List<Tuple2<String, byte[]>> byteCode) {
        Preconditions.checkNotNull(byteCode);
        Class<?> clazz = null;
        for (final Tuple2<String, byte[]> c : byteCode) {
            final String className = c._1;
            final byte[] compressedBinary = c._2;
            try {
                clazz = Class.forName(className);
            } catch (ClassNotFoundException e) {
                clazz = classLoader.buildClassFromByteArray(className, compressor.decompress(compressedBinary));
            }
        }
        return clazz;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    @SuppressWarnings("unchecked")
    private byte[] readByteCode(final Class<?> clazz) {
        String topLevelClazzName = null;
        Class<?> enclosingClazz = clazz.getEnclosingClass();
        while (enclosingClazz != null) {
            topLevelClazzName = enclosingClazz.getSimpleName();
            enclosingClazz = enclosingClazz.getEnclosingClass();
        }
        if (topLevelClazzName == null)
            topLevelClazzName = clazz.getSimpleName();

        final StringTokenizer tokenizer = new StringTokenizer(clazz.getCanonicalName(), ".");
        final StringBuilder pathBuilder = new StringBuilder();
        boolean isClazzFilename = false;
        while (tokenizer.hasMoreTokens()) {
            final String token = tokenizer.nextToken();
            if (!token.equals(topLevelClazzName) && !isClazzFilename)
                pathBuilder.append(token).append("/");
            else {
                pathBuilder.append(token);
                if (tokenizer.hasMoreTokens())
                    pathBuilder.append("$");
                isClazzFilename = true;
            }
        }

        final String filePath = clazz.getProtectionDomain().getCodeSource().getLocation().getPath() + pathBuilder.toString() + ".class";
        final File clazzFile = new File(filePath.replace("%20", " "));
        FileInputStream fis = null;
        byte[] clazzData = null;
        try {
            fis = new FileInputStream(clazzFile);
            clazzData = new byte[(int) clazzFile.length()]; // TODO: use IOUtils, Apache Commons!
            fis.read(clazzData);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            try {
                if (fis != null)
                    fis.close();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        return clazzData;
    }
}