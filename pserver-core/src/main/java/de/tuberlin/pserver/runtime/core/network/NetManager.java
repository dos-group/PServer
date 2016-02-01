package de.tuberlin.pserver.runtime.core.network;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.core.events.EventDispatcher;
import de.tuberlin.pserver.runtime.core.infra.InfrastructureManager;
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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class NetManager extends EventDispatcher {

    // TODO: ChannelGroup: Single outbound pipeline pass possible?

    public static final String OUT_CHANNELS = "out_channels";

    public static final String IN_CHANNELS  = "in_channels";

    public static final String ALL_CHANNELS = "broadcast_channels";

    // -------------------------------------------------------

    private final InfrastructureManager infraManager;

    private final MachineDescriptor machine;

    private final NetChannelConfig  channelConfig;

    private final EventLoopGroup    bossGroup;

    private final EventLoopGroup    workerGroup;

    private final ByteBufAllocator  allocator;

    private final ChannelGroup      outChannels;

    private final ChannelGroup      inChannels;

    private final ChannelGroup      allChannels;

    private final Map<MachineDescriptor, List<NetChannel>> activeChannels;

    // -------------------------------------------------------

    public NetManager(InfrastructureManager infraManager, MachineDescriptor machine, int numThreads) {
        super(true);
        this.infraManager       = Preconditions.checkNotNull(infraManager);
        this.machine            = Preconditions.checkNotNull(machine);
        this.channelConfig      = new NetChannelConfig();
        this.bossGroup          = new NioEventLoopGroup(1);
        this.workerGroup        = new NioEventLoopGroup(numThreads);
        this.allocator          = NetBufferAllocator.create(); // TODO: Change this!
        this.activeChannels     = new ConcurrentHashMap<>();
        this.outChannels        = new DefaultChannelGroup(OUT_CHANNELS, GlobalEventExecutor.INSTANCE);
        this.inChannels         = new DefaultChannelGroup(IN_CHANNELS,  GlobalEventExecutor.INSTANCE);
        this.allChannels        = new DefaultChannelGroup(ALL_CHANNELS, GlobalEventExecutor.INSTANCE);
    }

    @Override
    public void deactivate() {
        try {
            stop();
            super.deactivate();
        } catch (Exception e) {
            throw new IllegalStateException();
        }
    }

    // -------------------------------------------------------

    private void defineServerPipeline(ChannelPipeline pipeline) {
        pipeline.addLast(
                //new Lz4FrameEncoder(true),
                //new Lz4FrameDecoder(true),
                new NetKryoEncoder(),
                new NetKryoDecoder(),
                //new ObjectEncoder(),
                //new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(getClass().getClassLoader())),
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
                //new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(getClass().getClassLoader())),
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
        srvBootstrap.bind(machine.port).sync();
    }

    public void stop() throws Exception {
        closeAllChannels();
        bossGroup.shutdownGracefully().sync();
        workerGroup.shutdownGracefully().sync();
    }

    public NetChannel connect(MachineDescriptor descriptor) {
        try {
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

        } catch(Exception e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
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

    public void connectToAll(List<MachineDescriptor> in, List<MachineDescriptor> out) throws Exception {
        for (MachineDescriptor nd : out) {
            connect(nd);
        }
        for (;;) {
            boolean isConnected = true;
            for (MachineDescriptor nd : in) {
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

    public MachineDescriptor getMachineDescriptor() {
        return machine;
    }

    // -------------------------------------------------------

    public void dispatchEventAt(final int[] nodeIDs, final NetEvent event) {
        for (int nodeID : nodeIDs) {
            final MachineDescriptor md = infraManager.getMachine(nodeID);
            if (md != null)
                dispatchEventAt(md, event);
        }
    }

    public void dispatchEventAt(UUID machineID, NetEvent event) { dispatchEventAt(infraManager.getMachine(machineID), event, 0); }
    public void dispatchEventAt(MachineDescriptor netDescriptor, NetEvent event) { dispatchEventAt(netDescriptor, event, 0); }
    public void dispatchEventAt(MachineDescriptor netDescriptor, NetEvent event, int channelIdx) {
        Preconditions.checkNotNull(netDescriptor);
        NetChannel netChannel = activeChannels.get(netDescriptor).get(channelIdx);
        event.netChannel = Preconditions.checkNotNull(netChannel);
        event.dstMachineID = netDescriptor.machineID;
        event.srcMachineID = machine.machineID;
        netChannel.sendMsg(event);
    }

    public void broadcastEvent(final NetEvent event) {
        for (final MachineDescriptor md : infraManager.getMachines()) {
            if (!machine.machineID.equals(md.machineID))
                dispatchEventAt(md, event);
        }
    }

    /*public <T> void addChannelHandler(Class<T> msgType, NetChannelHandler<T> handler) {
        addEventListener(msgType.getName(), (Event event) -> handler.handle(
                ((NetChannelEvent)event).netChannel, ((T)event.getPayload())
        ));
    }*/

    // -------------------------------------------------------

    private NetChannel registerNetChannel(MachineDescriptor descriptor, Channel channel,  NetChannel.NetChannelType type) {
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
            netChannel.channel.writeAndFlush(machine, netChannel.channel.voidPromise());
        else if (type == NetChannel.NetChannelType.CHANNEL_IN)
            netChannel.channel.writeAndFlush(descriptor, netChannel.channel.voidPromise());

        return netChannel;
    }

    // ---------------------------------------------------
    // Handshake Protocol.
    // ---------------------------------------------------

    private final class NetHandshakeRequestHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof MachineDescriptor) {
                registerNetChannel((MachineDescriptor) msg, ctx.channel(), NetChannel.NetChannelType.CHANNEL_IN);
                ctx.pipeline().remove(this);
            } else {
                throw new IllegalStateException("Wrong handshake protocol.");
            }
        }
    }

    private final class NetHandshakeResponseHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof MachineDescriptor && msg.equals(machine))
                ctx.pipeline().remove(this);
            else
                throw new IllegalStateException("Wrong handshake protocol.");
        }
    }
}
