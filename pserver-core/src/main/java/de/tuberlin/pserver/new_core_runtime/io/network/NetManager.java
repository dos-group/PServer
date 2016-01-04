package de.tuberlin.pserver.new_core_runtime.io.network;

import de.tuberlin.pserver.new_core_runtime.events.Event;
import de.tuberlin.pserver.new_core_runtime.events.EventDispatcher;
import de.tuberlin.pserver.new_core_runtime.io.infrastructure.NetBufferAllocator;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class NetManager extends EventDispatcher {

    // TODO: ChannelGroup: Single outbound pipeline pass possible?

    public static final String OUT_CHANNELS = "out_channels";

    public static final String IN_CHANNELS  = "in_channels";

    public static final String ALL_CHANNELS = "broadcast_channels";

    // -------------------------------------------------------

    private final NetDescriptor     thisNetDescriptor;

    private final NetChannelConfig  channelConfig;

    private final EventLoopGroup    bossGroup;

    private final EventLoopGroup    workerGroup;

    private final ByteBufAllocator  allocator;

    private final ChannelGroup      outChannels;

    private final ChannelGroup      inChannels;

    private final ChannelGroup      allChannels;

    private final Map<NetDescriptor, List<NetChannel>> activeChannels;

    // -------------------------------------------------------

    public NetManager(NetDescriptor thisNetDescriptor, int numThreads) {
        super(true);
        this.thisNetDescriptor  = thisNetDescriptor;
        this.channelConfig      = new NetChannelConfig();
        this.bossGroup          = new NioEventLoopGroup(1);
        this.workerGroup        = new NioEventLoopGroup(numThreads);
        this.allocator          = NetBufferAllocator.create(); // TODO: Change this!
        this.activeChannels     = new ConcurrentHashMap<>();
        this.outChannels        = new DefaultChannelGroup(OUT_CHANNELS, GlobalEventExecutor.INSTANCE);
        this.inChannels         = new DefaultChannelGroup(IN_CHANNELS,  GlobalEventExecutor.INSTANCE);
        this.allChannels        = new DefaultChannelGroup(ALL_CHANNELS, GlobalEventExecutor.INSTANCE);
    }

    // -------------------------------------------------------

    private void defineServerPipeline(ChannelPipeline pipeline) {
        pipeline.addLast(
                //new Lz4FrameEncoder(true),
                //new Lz4FrameDecoder(true),
                new NetKryoEncoder(),
                new NetKryoDecoder(),
                //new ObjectEncoder(),
                //new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                new NetHandshakeRequestHandler(),
                new NetMessageHandler(NetManager.this)
        );
    }

    private void defineClientPipeline(ChannelPipeline pipeline) {
        pipeline.addLast(
                //new Lz4FrameEncoder(true),
                //new Lz4FrameDecoder(true),
                new NetKryoEncoder(),
                new NetKryoDecoder(),
                //new ObjectEncoder(),
                //new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                new NetHandshakeResponseHandler(),
                new NetMessageHandler(NetManager.this)
        );
    }

    // -------------------------------------------------------

    public void start() throws Exception {
        ServerBootstrap srvBootstrap = new ServerBootstrap();
        srvBootstrap.group(bossGroup, workerGroup);
        srvBootstrap.channel(NioServerSocketChannel.class);
        srvBootstrap.handler(new LoggingHandler(LogLevel.INFO));
        srvBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {

            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                channelConfig.configureChannel(ch);
                ch.config().setAllocator(allocator);
                defineServerPipeline(ch.pipeline());
            }
        });

        // Bind and start to accept incoming connections.
        srvBootstrap.bind(thisNetDescriptor.port).sync().channel().closeFuture().sync();
    }

    public void stop() throws Exception {
        closeAllChannels();
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    public NetChannel connect(NetDescriptor descriptor) throws Exception {
        Bootstrap cliBootstrap = new Bootstrap();
        cliBootstrap.group(workerGroup);
        cliBootstrap.channel(NioSocketChannel.class);
        cliBootstrap.handler(new ChannelInitializer<SocketChannel>() {

            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                channelConfig.configureChannel(ch);
                ch.config().setAllocator(allocator);
                defineClientPipeline(ch.pipeline());
            }
        });

        // Start the connection attempt.
        ChannelFuture channelFuture = cliBootstrap.connect(descriptor.hostname, descriptor.port);
        channelFuture.awaitUninterruptibly();
        // Now we are sure the future is completed.
        assert channelFuture.isDone();
        if (channelFuture.isCancelled()) {
            // Connection attempt cancelled by user.
            throw new IllegalStateException("Connection attempt cancelled");
        } else if (!channelFuture.isSuccess()) {
            // Connection attempt was not successful.
            throw new IllegalStateException(channelFuture.cause());
        } else {
            return registerNetChannel(descriptor, channelFuture.channel(), NetChannel.NetChannelType.CHANNEL_OUT);
        }
    }

    public void closeAllChannels() throws Exception {
        ChannelGroupFuture channelGroupFuture = allChannels.close().await();
        channelGroupFuture.awaitUninterruptibly();
        // Now we are sure the future is completed.
        assert channelGroupFuture.isDone();
        if (!channelGroupFuture.isSuccess()) {
            // Connection attempt was not successful.
            throw new IllegalStateException(channelGroupFuture.cause());
        }
    }

    public void connectToAll(List<NetDescriptor> in, List<NetDescriptor> out) throws Exception {
        for (NetDescriptor nd : out) {
            connect(nd);
        }
        for (;;) {
            boolean isConnected = true;
            for (NetDescriptor nd : in) {
                if (!activeChannels.containsKey(nd)) {
                    isConnected = false;
                    break;
                }
            }
            if (isConnected)
                break;
            else
                Thread.sleep(100);
        }
    }

    public void sendMsg(NetDescriptor netDescriptor, Object msg) { sendMsg(netDescriptor, msg, 0); }
    public void sendMsg(NetDescriptor netDescriptor, Object msg, int channelIdx) {
        NetChannel netChannel = activeChannels.get(netDescriptor).get(channelIdx);
        netChannel.sendMsg(msg);
    }

    public <T> void addMsgHandler(Class<T> msgType, NetChannelHandler<T> handler) {
        addEventListener(msgType.getName(), (Event event) -> handler.handle(
                ((NetChannelEvent)event).netChannel, ((T)event.getPayload())
        ));
    }

    private NetChannel registerNetChannel(NetDescriptor descriptor, Channel channel,  NetChannel.NetChannelType type) {
        boolean channelGroupRegistration = false;
        if (type == NetChannel.NetChannelType.CHANNEL_IN)
            channelGroupRegistration = inChannels.add(channel);
        else if (type == NetChannel.NetChannelType.CHANNEL_OUT)
            channelGroupRegistration = outChannels.add(channel);
        channelGroupRegistration &= allChannels.add(channel);

        if (!channelGroupRegistration)
            throw new IllegalStateException("Could not register to channel groups.");

        NetChannel netChannel = new NetChannel(descriptor, type, channel);
        List<NetChannel> netChannelList = activeChannels.get(descriptor);
        if (netChannelList == null) {
            netChannelList = new ArrayList<>();
            activeChannels.put(descriptor, netChannelList);
        }
        netChannelList.add(netChannel);
        // Inject runtime netChannel object in associated message read handler.
        NetMessageHandler msgReadHandler = (NetMessageHandler)
                channel.pipeline().context(NetMessageHandler.class).handler();
        msgReadHandler.setNetChannel(netChannel);
        // Send thisNetDescriptor as handshake object.
        if (type == NetChannel.NetChannelType.CHANNEL_OUT)
            netChannel.channel.writeAndFlush(thisNetDescriptor, netChannel.channel.voidPromise());
        else if (type == NetChannel.NetChannelType.CHANNEL_IN)
            netChannel.channel.writeAndFlush(descriptor, netChannel.channel.voidPromise());

        return netChannel;
    }

    private final class NetHandshakeRequestHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof NetDescriptor) {
                registerNetChannel((NetDescriptor) msg, ctx.channel(), NetChannel.NetChannelType.CHANNEL_IN);
                ctx.pipeline().remove(this);
            } else {
                throw new IllegalStateException("Wrong handshake protocol.");
            }
        }
    }

    private final class NetHandshakeResponseHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof NetDescriptor && msg.equals(thisNetDescriptor))
                ctx.pipeline().remove(this);
            else
                throw new IllegalStateException("Wrong handshake protocol.");
        }
    }
}
