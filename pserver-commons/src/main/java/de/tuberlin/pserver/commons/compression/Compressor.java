package de.tuberlin.pserver.commons.compression;

import com.google.common.base.Preconditions;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public interface Compressor extends Serializable {

    public abstract byte[] compress(final byte[] data);

    public abstract byte[] decompress(final byte[] data);

    public abstract byte[] decompress(final byte[] data, final int decompressedLength);

    public abstract byte[] decompress(final byte[] src, final byte[] dst);

    // ---------------------------------------------------

    public enum CompressionType {

        NO_COMPRESSION,

        JAVA_COMPRESSION,

        LZ4_COMPRESSION;
    }

    // ---------------------------------------------------

    public static final class Factory {

        private Factory() {}

        public static Compressor create(final CompressionType type) {
            switch(Preconditions.checkNotNull(type)) {
                case NO_COMPRESSION:    return new NoCompressor();
                case JAVA_COMPRESSION:  return new JavaCompressor();
                case LZ4_COMPRESSION:   return new Lz4Compressor();
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }

    // ---------------------------------------------------

    static final class NoCompressor implements Compressor {
        @Override  public byte[] compress(final byte[] data) { return data; }
        @Override  public byte[] decompress(final byte[] data) { return data; }
        @Override  public byte[] decompress(byte[] data, int decompressedLength) { return data; }
        @Override  public byte[] decompress(byte[] src, byte[] dst) { System.arraycopy(src, 0, dst, 0, src.length); return dst; }
    }

    // ---------------------------------------------------

    static final class Lz4Compressor implements Compressor {

        private static final LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();

        private static final LZ4FastDecompressor decompressor = LZ4Factory.fastestInstance().fastDecompressor();

        // ---------------------------------------------------

        @Override
        public byte[] compress(byte[] data) {
            Preconditions.checkNotNull(data);
            int maxCompressedLength = compressor.maxCompressedLength(data.length);
            byte[] compressed = new byte[maxCompressedLength];
            compressor.compress(data, 0, data.length, compressed, 0, maxCompressedLength);
            return compressed;
        }

        @Override
        public byte[] decompress(byte[] data) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] decompress(final byte[] data, final int decompressedLength) {
            Preconditions.checkNotNull(null);
            final byte[] restored = new byte[decompressedLength];
            decompressor.decompress(data, 0, restored, 0, decompressedLength);
            return restored;
        }

        @Override
        public byte[] decompress(final byte[] src, final byte[] dst) {
            decompressor.decompress(src, dst);
            return dst;
        }
    }
    // ---------------------------------------------------

    static final class JavaCompressor implements Compressor {

        @Override
        public byte[] compress(final byte[] data) {
            Preconditions.checkNotNull(data);
            final Deflater deflater = new Deflater();
            deflater.setInput(data);
            final ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);
            deflater.finish();
            final byte[] buffer = new byte[1024];
            while (!deflater.finished()) {
                int count = deflater.deflate(buffer); // returns the generated code... index
                baos.write(buffer, 0, count);
            }
            try {
                baos.close();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            final byte[] output = baos.toByteArray();
            deflater.end();
            return output;
        }

        @Override
        public byte[] decompress(final byte[] data) {
            Preconditions.checkNotNull(data);
            final Inflater inflater = new Inflater();
            inflater.setInput(data);
            final ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);
            byte[] buffer = new byte[1024];
            try {
                while (!inflater.finished()) {
                    int count = inflater.inflate(buffer);
                    baos.write(buffer, 0, count);
                }
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            } finally {
                try {
                    baos.close();
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            }
            final byte[] output = baos.toByteArray();
            inflater.end();
            return output;
        }

        @Override
        public byte[] decompress(final byte[] data, final int decompressedLength) { return decompress(data); }

        @Override
        public byte[] decompress(byte[] src, byte[] dst) {
            throw new UnsupportedOperationException();
        }
    }
}