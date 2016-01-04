package de.tuberlin.pserver.new_core_runtime.io.network;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.UnsafeInput;
import de.tuberlin.pserver.new_core_runtime.io.serializer.KryoFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import static io.netty.buffer.Unpooled.buffer;


public final class NetKryoDecoder extends LengthFieldBasedFrameDecoder {

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
        deserializationBuffer.clear();
        deserializationBuffer.release();
        return msg;
    }

    @Override
    protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
        return buffer.slice(index, length);
    }
}
