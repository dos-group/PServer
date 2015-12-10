package de.tuberlin.pserver.examples.experiments.micro_benchmarks;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import io.netty.buffer.ByteBuf;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import static io.netty.buffer.Unpooled.*;

import java.util.Random;

public class LZ4CompressionBenchmark {

    public static void main(final String[] args) throws Exception {

        // 8589934592 = 8GB

        final Kryo kryo = new Kryo();

        final ByteBuf outBuffer = buffer(Integer.MAX_VALUE - 100);

        final ByteBuf inBuffer = buffer(Integer.MAX_VALUE - 100);

        final LZ4Compressor compressor = LZ4Factory.safeInstance().fastCompressor();

        final LZ4FastDecompressor decompressor = LZ4Factory.safeInstance().fastDecompressor();

        // -------------------------------------------------

        final int size_mb = 500;

        final int num_elements = (int)((1048576.0 * size_mb) / Double.BYTES);

        final double[] model = new double[num_elements];

        // -------------------------------------------------

        final Random rand = new Random();

        for (int i = 0; i < num_elements; ++i) {

            model[i] = rand.nextGaussian() * Double.MAX_VALUE;
        }

        // -------------------------------------------------

        final int num_of_runs = 5;

        for (int i = 0; i < num_of_runs; ++i) {

            final long start = System.currentTimeMillis();

            // Serialization.

            Output output = new UnsafeOutput(outBuffer.array());

            kryo.writeClassAndObject(output, model);

            final ByteBuf serialized_model = outBuffer.slice(0, output.position());

            output.flush();

            output.close();

            //System.out.println("serialized model in buffer region: "
            //        + serialized_model.arrayOffset() + " - " + (serialized_model.arrayOffset() + serialized_model.capacity() - 1));

            // Compression.

            int maxDestLen = compressor.maxCompressedLength(serialized_model.capacity());

            ByteBuf compressed_out_model = outBuffer.slice(serialized_model.capacity(), maxDestLen);

            int compressedSize = compressor.compress(
                    serialized_model.array(),
                    serialized_model.arrayOffset(),
                    serialized_model.capacity(),
                    compressed_out_model.array(),
                    compressed_out_model.arrayOffset(),
                    compressed_out_model.capacity()
            );

            compressed_out_model = compressed_out_model.slice(0, compressedSize);

            //System.out.println("compressed model in buffer region: "
            //        + compressed_out_model.arrayOffset() + " - " + (compressed_out_model.arrayOffset() + compressed_out_model.capacity()));

            final long stop = System.currentTimeMillis();

            if (i == num_of_runs - 1) {

                final double uncompressed_size = model.length * Double.BYTES;

                System.out.println("mb:           " + size_mb);

                System.out.println("length:       " + num_elements + " => "+ (num_elements * Double.BYTES));

                System.out.println("time:         " + ((stop - start) / 1000.0) + "s");

                System.out.println("uncompressed: " + (int)uncompressed_size);

                System.out.println("compressed:   " + compressedSize);

                System.out.println("ratio:        " + (100 - ((compressedSize / uncompressed_size) * 100.0)) + "%");

                // -------------------------------------------------

                // Transport.

                ByteBuf compressed_in_model = inBuffer.slice(0, compressedSize);

                compressed_in_model.clear(); // TODO: WHY?

                compressed_in_model.writeBytes(
                        compressed_out_model.array(),
                        compressed_out_model.arrayOffset(),
                        compressed_out_model.capacity()
                );

                int serialized_model_size = serialized_model.capacity();

                ByteBuf serialized_in_model = inBuffer.slice(compressedSize + 1, serialized_model_size);

                // Decompression.

                decompressor.decompress(
                        compressed_out_model.array(),
                        compressed_out_model.arrayOffset(),
                        serialized_in_model.array(),
                        serialized_in_model.arrayOffset(),
                        serialized_in_model.capacity()
                );

                // Deserialization.

                Input input = new UnsafeInput(
                        serialized_in_model.array(),
                        serialized_in_model.arrayOffset(),
                        serialized_in_model.capacity()
                );

                double[] deserialized_model = (double[]) kryo.readClassAndObject(input);

                input.close();

                // Validation.

                for (int j = 0; j < num_elements; ++j) {

                    if (model[j] != deserialized_model[j]) {

                        System.out.println("FAILURE");

                        break;
                    }
                }
            }
        }
    }
}
