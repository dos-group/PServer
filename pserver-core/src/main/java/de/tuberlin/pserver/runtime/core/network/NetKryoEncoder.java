package de.tuberlin.pserver.runtime.core.network;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import de.tuberlin.pserver.runtime.core.serializer.KryoFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;


public final class NetKryoEncoder extends MessageToByteEncoder<Object> {

    private final static int ENCODING_BUFFER =  1024 * 1024 * 200; // 200MB

    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];


    private ThreadLocal kryoThreadLocal = new ThreadLocal<Kryo>() {
        @Override protected Kryo initialValue() {
            return KryoFactory.INSTANCE.create();
        }
    };

    private ThreadLocal outputThreadLocal = new ThreadLocal<Output>() {
        @Override protected Output initialValue() {
            return new UnsafeOutput();
        }
    };

    private ThreadLocal encodingBuffer = new ThreadLocal<byte[]>() {
        @Override protected byte[] initialValue() {
            return new byte[ENCODING_BUFFER];
        }
    };

    public NetKryoEncoder() {
        // Ensure heap buffers are used when pass into encode(...) method.
        // This way you can access the backing array directly.
        super(false);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        try {

            final Kryo    kryo = (Kryo)kryoThreadLocal.get();
            final Output  output = (Output)outputThreadLocal.get();
            final byte[]  encodingBuf = (byte[])encodingBuffer.get();

            output.setBuffer(encodingBuf);
            int startPos = output.position();
            kryo.writeClassAndObject(output, msg);
            int len = output.position() - startPos;
            output.close();
            out.writeInt(len);
            out.writeBytes(encodingBuf, 0, len);

            /*// Keep offset of output buffer.
            int startIdx = out.writerIndex();
            ByteBufOutputStream outStream = new ByteBufOutputStream(out);
            // First, placeholder to output buffer.
            outStream.write(LENGTH_PLACEHOLDER);
            // Delegate the backing array to kryo output.
            output.setBuffer(outStream.buffer().array());
            output.setOutputStream(outStream);
            // Serialize data.
            int startPos = output.position();
            kryo.writeClassAndObject(output, msg);
            int len = output.position() - startPos;
            // Write serialized data on backing array.
            output.flush();
            output.close();
            // Write size in placeholder position.
            out.setInt(startIdx, len);
            out.writerIndex(startIdx + 4 + len);*/

        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}