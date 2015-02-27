package de.tuberlin.pserver.core.memory;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.utils.UnsafeOp;

import java.io.Serializable;

public class Buffer implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final byte[] buffer;

    protected int internalOffset = 0;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public Buffer(final int size) {
        Preconditions.checkArgument(size > 0);
        this.buffer = new byte[size];
    }

    public Buffer(final byte[] buffer) {
        Preconditions.checkNotNull(buffer);
        this.buffer = buffer;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public int size() { return buffer.length; }

    public byte[] getRawData() { return buffer; }

    // -----------------------

    public void putShort(final int offset, final short value) {
        UnsafeOp.unsafe.putShort(buffer, UnsafeOp.byteArrayOffset + offset, value);
        internalOffset = offset + Types.PrimitiveType.SHORT.size;
    }

    public void putInt(final int offset, final int value) {
        UnsafeOp.unsafe.putInt(buffer, UnsafeOp.byteArrayOffset + offset, value);
        internalOffset = offset + Types.PrimitiveType.INT.size;
    }

    public void putLong(final int offset, final long value) {
        UnsafeOp.unsafe.putLong(buffer, UnsafeOp.byteArrayOffset + offset, value);
        internalOffset = offset + Types.PrimitiveType.LONG.size;
    }

    public void putFloat(final int offset, final float value) {
        UnsafeOp.unsafe.putFloat(buffer, UnsafeOp.byteArrayOffset + offset, value);
        internalOffset = offset + Types.PrimitiveType.FLOAT.size;
    }

    public void putDouble(final int offset, final double value) {
        UnsafeOp.unsafe.putDouble(buffer, UnsafeOp.byteArrayOffset + offset, value);
        internalOffset = offset + Types.PrimitiveType.DOUBLE.size;
    }

    // -----------------------

    public void put(final int offset, final short value) {
        UnsafeOp.unsafe.putShort(buffer, UnsafeOp.byteArrayOffset + offset, value);
    }

    public void put(final int offset, final int value) {
        UnsafeOp.unsafe.putInt(buffer, UnsafeOp.byteArrayOffset + offset, value);
    }

    public void put(final int offset, final long value) {
        UnsafeOp.unsafe.putLong(buffer, UnsafeOp.byteArrayOffset + offset, value);
    }

    public void put(final int offset, final float value) {
        UnsafeOp.unsafe.putFloat(buffer, UnsafeOp.byteArrayOffset + offset, value);
    }

    public void put(final int offset, final double value) {
        UnsafeOp.unsafe.putDouble(buffer, UnsafeOp.byteArrayOffset + offset, value);
    }

    // -----------------------

    public void putShort(final short value) {
        UnsafeOp.unsafe.putShort(buffer, UnsafeOp.byteArrayOffset + internalOffset, value);
        internalOffset += Types.PrimitiveType.SHORT.size;
    }

    public void putInt(final int value) {
        UnsafeOp.unsafe.putInt(buffer, UnsafeOp.byteArrayOffset + internalOffset, value);
        internalOffset += Types.PrimitiveType.INT.size;
    }

    public void putLong(final long value) {
        UnsafeOp.unsafe.putLong(buffer, UnsafeOp.byteArrayOffset + internalOffset, value);
        internalOffset += Types.PrimitiveType.LONG.size;
    }

    public void putFloat(final float value) {
        UnsafeOp.unsafe.putFloat(buffer, UnsafeOp.byteArrayOffset + internalOffset, value);
        internalOffset += Types.PrimitiveType.FLOAT.size;
    }

    public void putDouble(final double value) {
        UnsafeOp.unsafe.putDouble(buffer, UnsafeOp.byteArrayOffset + internalOffset, value);
        internalOffset += Types.PrimitiveType.DOUBLE.size;
    }

    // -----------------------

    public short getShort(final int offset) {
        return UnsafeOp.unsafe.getShort(buffer, UnsafeOp.byteArrayOffset + offset);
    }

    public int getInt(final int offset) {
        return UnsafeOp.unsafe.getInt(buffer, UnsafeOp.byteArrayOffset + offset);
    }

    public long getLong(final int offset) {
        return UnsafeOp.unsafe.getLong(buffer, UnsafeOp.byteArrayOffset + offset);
    }

    public float getFloat(final int offset) {
        return UnsafeOp.unsafe.getFloat(buffer, UnsafeOp.byteArrayOffset + offset);
    }

    public double getDouble(final int offset) {
        return UnsafeOp.unsafe.getDouble(buffer, UnsafeOp.byteArrayOffset + offset);
    }

    // -----------------------

    public short getShort() {
        final short e = UnsafeOp.unsafe.getShort(buffer, UnsafeOp.byteArrayOffset + internalOffset);
        internalOffset += Types.PrimitiveType.SHORT.size;
        return e;
    }

    public int getInt() {
        final int e = UnsafeOp.unsafe.getInt(buffer, UnsafeOp.byteArrayOffset + internalOffset);
        internalOffset += Types.PrimitiveType.INT.size;
        return e;
    }

    public long getLong() {
        final long e = UnsafeOp.unsafe.getLong(buffer, UnsafeOp.byteArrayOffset + internalOffset);
        internalOffset += Types.PrimitiveType.LONG.size;
        return e;
    }

    public float getFloat() {
        final float e = UnsafeOp.unsafe.getFloat(buffer, UnsafeOp.byteArrayOffset + internalOffset);
        internalOffset += Types.PrimitiveType.FLOAT.size;
        return e;
    }

    public double getDouble() {
        final double e = UnsafeOp.unsafe.getDouble(buffer, UnsafeOp.byteArrayOffset + internalOffset);
        internalOffset += Types.PrimitiveType.DOUBLE.size;
        return e;
    }
}
