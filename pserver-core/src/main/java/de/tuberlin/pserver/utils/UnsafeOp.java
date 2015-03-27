package de.tuberlin.pserver.utils;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class UnsafeOp {

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

    public static final int BYTE_ARRAY_OFFSET      = unsafe.arrayBaseOffset(byte[].class);

    public static final int SHORT_ARRAY_OFFSET     = unsafe.arrayBaseOffset(short[].class);

    public static final int INT_ARRAY_OFFSET       = unsafe.arrayBaseOffset(int[].class);

    public static final int LONG_ARRAY_OFFSET      = unsafe.arrayBaseOffset(long[].class);

    public static final int FLOAT_ARRAY_OFFSET     = unsafe.arrayBaseOffset(float[].class);

    public static final int DOUBLE_ARRAY_OFFSET    = unsafe.arrayBaseOffset(double[].class);

    public static final int BOOLEAN_ARRAY_OFFSET   = unsafe.arrayBaseOffset(boolean[].class);
}
