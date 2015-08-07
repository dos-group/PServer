package de.tuberlin.pserver.commons.unsafe;

import com.google.common.base.Preconditions;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

public final class UnsafeOp {

    // Disallow instantiation.
    private UnsafeOp() {}

    public static final Unsafe unsafe;

    static {
        try {
            final Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static final int BOOLEAN_TYPE_SIZE = 4; // JVM dependent!

    public static final int BYTE_ARRAY_OFFSET      = unsafe.arrayBaseOffset(byte[].class);
    public static final int SHORT_ARRAY_OFFSET     = unsafe.arrayBaseOffset(short[].class);
    public static final int INT_ARRAY_OFFSET       = unsafe.arrayBaseOffset(int[].class);
    public static final int LONG_ARRAY_OFFSET      = unsafe.arrayBaseOffset(long[].class);
    public static final int FLOAT_ARRAY_OFFSET     = unsafe.arrayBaseOffset(float[].class);
    public static final int DOUBLE_ARRAY_OFFSET    = unsafe.arrayBaseOffset(double[].class);
    public static final int BOOLEAN_ARRAY_OFFSET   = unsafe.arrayBaseOffset(boolean[].class);

    public static final long ARRAY_HEADER_TYPE_TAG_OFFSET = 8L;   // JVM dependent!
    public static final long ARRAY_HEADER_TYPE_SIZE_OFFSET = 12L; // JVM dependent!

    public static final short TYPE_TAG_BOOLEAN  = unsafe.getShort(new boolean[0], ARRAY_HEADER_TYPE_TAG_OFFSET);
    public static final short TYPE_TAG_BYTE     = unsafe.getShort(new byte[0],    ARRAY_HEADER_TYPE_TAG_OFFSET);
    public static final short TYPE_TAG_SHORT    = unsafe.getShort(new short[0],   ARRAY_HEADER_TYPE_TAG_OFFSET);
    public static final short TYPE_TAG_INT      = unsafe.getShort(new int[0],     ARRAY_HEADER_TYPE_TAG_OFFSET);
    public static final short TYPE_TAG_LONG     = unsafe.getShort(new long[0],    ARRAY_HEADER_TYPE_TAG_OFFSET);
    public static final short TYPE_TAG_FLOAT    = unsafe.getShort(new float[0],   ARRAY_HEADER_TYPE_TAG_OFFSET);
    public static final short TYPE_TAG_DOUBLE   = unsafe.getShort(new double[0],  ARRAY_HEADER_TYPE_TAG_OFFSET);

    public static <T1> short getPrimitiveSizeInBytes(final Class<T1> type) {
        Preconditions.checkState(type != null && type.isArray());
        final Class<?> elementType = type.getComponentType();
        if (elementType == boolean.class)     return BOOLEAN_TYPE_SIZE;
        else if (elementType == byte.class)   return Byte.SIZE;
        else if (elementType == short.class)  return Short.SIZE;
        else if (elementType == int.class)    return Integer.SIZE;
        else if (elementType == long.class)   return Long.SIZE;
        else if (elementType == float.class)  return Float.SIZE;
        else if (elementType == double.class) return Double.SIZE;
        throw new IllegalStateException();
    }

    public static <T1> short getPrimitiveTypeTag(final Class<T1> type) {
        Preconditions.checkState(type != null && type.isArray());
        final Class<?> elementType = type.getComponentType();
        if (elementType == boolean.class)     return TYPE_TAG_BOOLEAN;
        else if (elementType == byte.class)   return TYPE_TAG_BYTE;
        else if (elementType == short.class)  return TYPE_TAG_SHORT;
        else if (elementType == int.class)    return TYPE_TAG_INT;
        else if (elementType == long.class)   return TYPE_TAG_LONG;
        else if (elementType == float.class)  return TYPE_TAG_FLOAT;
        else if (elementType == double.class) return TYPE_TAG_DOUBLE;
        throw new IllegalStateException();
    }

    @SuppressWarnings("unchecked")
    public static <T1,T2> T2 primitiveArrayTypeCast(final T1 t1, final Class<T1> t1Type, final Class<T2> t2Type) {
        Preconditions.checkNotNull(t1);
        Preconditions.checkState(t1Type != null && t1Type.isArray());
        Preconditions.checkState(t2Type != null && t2Type.isArray());
        // Change type.
        final short t2TypeTag = getPrimitiveTypeTag(t2Type);
        unsafe.putShort(t1, ARRAY_HEADER_TYPE_TAG_OFFSET, t2TypeTag);
        // Change length.
        final short t1TypeSize = getPrimitiveSizeInBytes(t1Type);
        final short t2TypeSize = getPrimitiveSizeInBytes(t2Type);
        final int t1Size = unsafe.getInt(t1, ARRAY_HEADER_TYPE_SIZE_OFFSET);
        final int size = t1TypeSize < t2TypeSize
                ? t1Size / (t2TypeSize / t1TypeSize)
                : t1Size * (t1TypeSize / t2TypeSize);
        unsafe.putInt(t1, ARRAY_HEADER_TYPE_SIZE_OFFSET, size);
        final Object tmp = t1;
        final T2 t2Array = (T2)tmp;
        return t2Array;
    }
}
