package de.tuberlin.pserver.runtime.core.network;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import de.tuberlin.pserver.runtime.core.serializer.KryoFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;


public final class NetKryoEncoder extends MessageToByteEncoder<Object> {

    private final Kryo kryo = KryoFactory.INSTANCE.create();

    private Output output = new UnsafeOutput();

    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];

    public NetKryoEncoder() {
        // Ensure heap buffers are used when pass into encode(...) method.
        // This way you can access the backing array directly.
        super(false);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        // Keep offset of output buffer.
        int startIdx = out.writerIndex();
        ByteBufOutputStream outStream = new ByteBufOutputStream(out);
        // First, placeholder to output buffer.
        outStream.write(LENGTH_PLACEHOLDER);
        // Delegate the backing array to kryo output.
        output.setBuffer(outStream.buffer().array());
        output.setOutputStream(outStream);
        // Set offset on backing array.
        output.setPosition(out.arrayOffset());
        // Serialize data.
        kryo.writeClassAndObject(output, msg);
        // Write serialized data on backing array.
        output.flush();
        output.close();
        // Write size in placeholder position.
        int endIdx = out.writerIndex();
        out.setInt(startIdx, endIdx - startIdx - 4);
    }
}

/*public class NetKryoEncoder extends MessageToByteEncoder<Object> {

    private final Kryo kryo;

    public NetKryoEncoder() {
        this.kryo = KryoFactory.INSTANCE.create();
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object in, ByteBuf out) throws Exception {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        Output output = new Output(outStream, 4096);
        kryo.writeClassAndObject(output, in);
        output.flush();
        byte[] outArray = outStream.toByteArray();
        out.writeInt(outArray.length);
        out.writeBytes(outArray);
    }
}*/