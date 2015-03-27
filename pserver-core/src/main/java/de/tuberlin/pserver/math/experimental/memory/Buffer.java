package de.tuberlin.pserver.math.experimental.memory;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.utils.Compression;
import de.tuberlin.pserver.utils.UnsafeOp;

import java.io.Serializable;

public class Buffer implements Serializable {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public enum CompressionPolicy {

        NO_COMPRESSION,

        LZ4_COMPRESSION
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final int decompressedLength;

    protected byte[] data;

    protected CompressionPolicy compressionPolicy = CompressionPolicy.NO_COMPRESSION;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public Buffer(final int length) {
        Preconditions.checkArgument(length > 0);
        this.data = new byte[length];
        this.decompressedLength = length;
    }

    public Buffer(final byte[] data) {
        Preconditions.checkNotNull(data);
        this.data = data;
        this.decompressedLength = data.length;
    }

    public Buffer(final byte[] data,
                  final int decompressedLength,
                  final CompressionPolicy compressionPolicy) {

        Preconditions.checkNotNull(decompressedLength > 0);
        this.data = Preconditions.checkNotNull(data);
        this.decompressedLength = decompressedLength;
        this.compressionPolicy = Preconditions.checkNotNull(compressionPolicy);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public int size() { return data.length; }

    public byte[] getRawData() { return data; }

    public void setRawData(final byte[] data) { this.data = data; }

    // ---------------------------------------------------

    public void putShort(final int offset, final short value) { UnsafeOp.unsafe.putShort(data, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + offset), value); }

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

    // ---------------------------------------------------

    public static Buffer compressBuffer(final CompressionPolicy policy,
                                        final Buffer buffer) {
        Preconditions.checkNotNull(policy);
        Preconditions.checkNotNull(buffer);
        switch (policy) {
            case NO_COMPRESSION:
                return buffer;
            case LZ4_COMPRESSION:
                return new Buffer(
                        Compression.lz4Compress(buffer.data),
                        buffer.data.length,
                        CompressionPolicy.LZ4_COMPRESSION
                );
            default:
                throw new IllegalStateException();
        }
    }

    /** Creates no copy! */
    public static Buffer decompressBuffer(final Buffer buffer) {
        Preconditions.checkNotNull(buffer);
        switch (buffer.compressionPolicy) {
            case NO_COMPRESSION:
                return buffer;
            case LZ4_COMPRESSION: {
                buffer.data = Compression.lz4Decompress(buffer.data, buffer.decompressedLength);
                return buffer;
            }
            default:
                throw new IllegalStateException();
        }
    }
}
