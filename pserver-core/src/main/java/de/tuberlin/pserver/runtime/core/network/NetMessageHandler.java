package de.tuberlin.pserver.runtime.core.network;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * From the application standpoint, the primary component of Netty is the ChannelHandler,
 * which serves as the container for all application logic that applies to handling inbound
 * and outbound data.
 *
 * This mtxType receives inbound events and data to be handled by the framework.
 */
public final class NetMessageHandler extends SimpleChannelInboundHandler<Object> { // ChannelInboundHandlerAdapter

    private final NetManager netManager;

    private NetChannel netChannel;

    public NetMessageHandler(NetManager netManager) {
        this.netManager = netManager;
    }

    public void setNetChannel(NetChannel netChannel) {
        this.netChannel = netChannel;
    }

    //@Override
    //public void channelRead(ChannelHandlerContext ctx, Object msg) {
    //    netManager.dispatchEvent(new NetChannelEvent(netChannel, msg));
    //}

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof NetEvent) {
            NetEvent event = (NetEvent)msg;
            event.netChannel = netChannel;
            netManager.dispatchEvent(event);
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        writeIfPossible();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

    private void writeIfPossible() {
        while (!netChannel.writeQueue.isEmpty() && netChannel.channel.isWritable()) {
            netChannel.channel.writeAndFlush(
                    netChannel.writeQueue.poll(),
                    netChannel.channel.voidPromise()
            );
        }
    }
}