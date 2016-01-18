package de.tuberlin.pserver.runtime.core.network;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.UnsafeInput;
import de.tuberlin.pserver.runtime.core.serializer.KryoFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;


public class NetKryoDecoder extends ByteToMessageDecoder {

    private final static int DECODING_BUFFER =  1024 * 1024 * 20; // 20MB

    private ThreadLocal kryoThreadLocal = new ThreadLocal<Kryo>() {
        @Override protected Kryo initialValue() {
            return KryoFactory.INSTANCE.create();
        }
    };

    private ThreadLocal inputThreadLocal = new ThreadLocal<Input>() {
        @Override protected Input initialValue() {
            return new UnsafeInput();
        }
    };

    private ThreadLocal decodingBuffer = new ThreadLocal<byte[]>() {
        @Override protected byte[] initialValue() {
            return new byte[DECODING_BUFFER];
        }
    };

    public NetKryoDecoder() {}

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < 4)
            return;

        in.markReaderIndex();
        int len = in.readInt();
        if (in.readableBytes() < len) {
            in.resetReaderIndex();
            return;
        }

        final Kryo kryo = (Kryo)kryoThreadLocal.get();
        final Input input = (Input)inputThreadLocal.get();
        final byte[] buf = (byte[])decodingBuffer.get();

        in.readBytes(buf, 0, len);
        input.setBuffer(buf);
        Object object = kryo.readClassAndObject(input);
        input.close();
        out.add(object);
    }
}

/*public final class NetKryoDecoder extends LengthFieldBasedFrameDecoder {

    private final Kryo kryo = KryoFactory.INSTANCE.create();

    private final Input input = new UnsafeInput();

    public NetKryoDecoder() {
        super(Integer.MAX_VALUE, 0, 4, 0, 4);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        if (frame == null) {
            return null;
        }
        // Create deserialization buffer.
        ByteBuf deserializationBuffer = buffer(frame.capacity());
        // Read data from direct buffer.
        frame.readBytes(deserializationBuffer);
        // Access deserializationBuffer as input stream.
        ByteBufInputStream inStream = new ByteBufInputStream(deserializationBuffer);
        // Prepare kryo input.
        input.setBuffer(deserializationBuffer.array());
        input.setInputStream(inStream);
        // Deserialize data.
        final Object msg = kryo.readClassAndObject(input); // Creates object on "user" JVM heap.
        input.close();
        // Release deserialization buffer.
        deserializationBuffer.release();
        return msg;
    }

    @Override
    protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
        return buffer.slice(index, length);
    }
}*/