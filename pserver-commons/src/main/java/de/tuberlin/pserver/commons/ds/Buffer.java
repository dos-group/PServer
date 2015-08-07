package de.tuberlin.pserver.commons.ds;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.unsafe.UnsafeOp;

public class Buffer {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public byte[] data;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public Buffer(final byte[] data) {
        this.data = Preconditions.checkNotNull(data);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void putShort(final int offset, final short value) {
        UnsafeOp.unsafe.putShort(data, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + offset), value);
    }

    public void putInt(final int offset, final int value) { UnsafeOp.unsafe.putInt(data, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + offset), value); }

    public void putLong(final int offset, final long value) { UnsafeOp.unsafe.putLong(data, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + offset), value); }

    public void putFloat(final int offset, final float value) { UnsafeOp.unsafe.putFloat(data, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + offset), value); }

    public void putDouble(final int offset, final double value) { UnsafeOp.unsafe.putDouble(data, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + offset), value); }

    // ---------------------------------------------------

    public short getShort(final int offset) { return UnsafeOp.unsafe.getShort(data, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + offset)); }

    public int getInt(final int offset) { return UnsafeOp.unsafe.getInt(data, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + offset)); }

    public long getLong(final int offset) { return UnsafeOp.unsafe.getLong(data, (long) (UnsafeOp.BYTE_ARRAY_OFFSET + offset)); }

    public float getFloat(final int offset) { return UnsafeOp.unsafe.getFloat(data, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + offset)); }

    public double getDouble(final int offset) { return UnsafeOp.unsafe.getDouble(data, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + offset)); }
}