package de.tuberlin.pserver.utils;

import com.google.common.base.Preconditions;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public final class Compression {

    // ---------------------------------------------------

    public static byte[] compress(final byte[] data) {
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

    public static byte[] decompress(final byte[] data) {
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

    // ---------------------------------------------------

    private static final LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();

    private static final LZ4FastDecompressor decompressor = LZ4Factory.fastestInstance().fastDecompressor();

    public static byte[] lz4Compress(final byte[] data) {
        Preconditions.checkNotNull(data);
        int maxCompressedLength = compressor.maxCompressedLength(data.length);
        byte[] compressed = new byte[maxCompressedLength];
        /*final int compressedLength = */compressor.compress(data, 0, data.length, compressed, 0, maxCompressedLength);
        //System.out.println("========================================> length = " + data.length + " | compressedLength = " + compressedLength);
        return compressed;
    }

    public static byte[] lz4Decompress(final byte[] data, final int decompressedLength) {
        byte[] restored = new byte[decompressedLength];
        decompressor.decompress(data, 0, restored, 0, decompressedLength);
        return restored;
    }
}