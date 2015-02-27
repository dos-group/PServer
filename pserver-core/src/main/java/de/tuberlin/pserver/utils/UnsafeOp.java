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

    public static final long byteArrayOffset = unsafe.arrayBaseOffset(byte[].class);

    public static final long shortArrayOffset = unsafe.arrayBaseOffset(short[].class);

    public static final long intArrayOffset = unsafe.arrayBaseOffset(int[].class);

    public static final long longArrayOffset = unsafe.arrayBaseOffset(long[].class);

    public static final long floatArrayOffset = unsafe.arrayBaseOffset(float[].class);

    public static final long doubleArrayOffset = unsafe.arrayBaseOffset(double[].class);

    public static final long booleanArrayOffset = unsafe.arrayBaseOffset(boolean[].class);
}
