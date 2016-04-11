package de.tuberlin.pserver.runtime.core.network;

import io.netty.channel.Channel;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class NetChannel {

    public enum NetChannelType {

        CHANNEL_IN,

        CHANNEL_OUT
    }

    public final MachineDescriptor descriptor;

    public final NetChannelType type;

    public final Channel channel;

    public final Queue<Object> writeQueue;

    public NetChannel(MachineDescriptor descriptor, NetChannelType type, Channel channel) {
        this.descriptor = descriptor;
        this.type       = type;
        this.channel    = channel;
        this.writeQueue = new ConcurrentLinkedQueue<>();
    }

    public void sendMsg(Object msg) {
        writeQueue.add(msg);
        if (channel.isWritable()) {
            channel.eventLoop().execute(() -> {
                if (!writeQueue.isEmpty() && channel.isWritable()) {
                    Object data = writeQueue.poll();
                    channel.writeAndFlush(data, channel.voidPromise());
                }
            });
        }
    }

    public String toString() {
        return channel.id().toString();
    }
}