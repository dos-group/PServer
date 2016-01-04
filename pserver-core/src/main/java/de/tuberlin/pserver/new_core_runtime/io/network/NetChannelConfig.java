package de.tuberlin.pserver.new_core_runtime.io.network;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelOption;

public class NetChannelConfig {

    // TCP Tuning - http://www.onlamp.com/pub/a/onlamp/2005/11/17/tcp_tuning.html

    // HOW TCP WORKS

    // The most common network protocol used on the internet is the Transmission Control
    // Protocol, or TCP. TCP uses a "congestion window" to determine how many packets it
    // can send at one time. The larger the congestion window size, the higher the throughput.
    // The TCP "slow start" and "congestion avoidance" algorithms determine the size of the
    // congestion window. The maximum congestion window is related to the amount of buffer
    // space that the kernel allocates for each socket. For each socket there is a default
    // value for the buffer size, which programs can change by using a system library call
    // just before opening the socket. For some operating systems there is also a kernel-enforced
    // maximum buffer size. You can adjust the buffer size for both the sending and receiving
    // ends of the socket.

    // To achieve maximum throughput, it is critical to use optimal TCP socket buffer sizes
    // for the link you are using. If the buffers are too small, the TCP congestion window
    // will never open up fully, so the sender will be throttled. If the buffers are too large,
    // the sender can overrun the receiver, which will cause the receiver to drop packets and
    // the TCP congestion window to shut down. This is more likely to happen if the sending host
    // is faster than the receiving host. An overly large window on the sending side is not a big
    // problem as long as you have excess memory.

    // COMPUTING TCP BUFFER SIZE

    // Assuming there is no network congestion or packet loss, network throughput is directly
    // related to TCP buffer size and the network latency. Network latency is the amount of time
    // for a packet to traverse the network. To calculate maximum throughput:
    // (1) Throughput = buffer size / latency

    // Most networking experts agree that the optimal TCP buffer size for a given network link is
    // double the value for delay times bandwidth:
    // (2) buffer size = 2 * delay * bandwidth

    // The ping program will give you the round trip time (RTT) for the network link, which is twice
    // the delay, so the formula simplifies to:
    // (3) buffer size = RTT * bandwidth

    // NOTE: Linux assumes that half of the send/receive buffer is used for internal
    // kernel structures; thus the sysctls are twice what can be observed on the wire.


    // NETTY HIGH- AND LOW- WATERMARK




    private final boolean tcpKeepAlive            = true;

    private final boolean tcpNoDelay              = true;

    private final int soSndBuf                    = 1045678;

    private final int soRcvBuf                    = 1045678;

    private final int writeBufferHighWatermark    = 32 * 1024;

    private final int writeBufferLowWatermark     = 8 * 1024;


    public void configureChannel(Channel channel) {

        ChannelConfig channelConfig = channel.config();


        // The maximum writeQueue length for incoming connection indications (a request
        // to connect) is set to the backlog parameter. If a connection indication
        // arrives when the writeQueue is full, the connection is refused.
        //channelConfig.setOption(ChannelOption.SO_BACKLOG, );

        //channelConfig.setOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, );

        // Enables sending keep-alive packets for a socket connection.
        // Not supported on ATM sockets (results in an error).
        channelConfig.setOption(ChannelOption.SO_KEEPALIVE, tcpKeepAlive);

        // When NoDelay is false, a TcpClient does not send a packet over the
        // network until it has collected a significant amount of outgoing data.
        // Because of the amount of overhead in a TCP segment, sending small amounts
        // of data is inefficient. However, situations do exist where you need to
        // send very small amounts of data or expect immediate responses from each
        // packet you send. Your decision should weigh the relative importance of
        // network efficiency versus application requirements.
        channelConfig.setOption(ChannelOption.TCP_NODELAY, tcpNoDelay);

        // Specifies the total per-socket buffer space reserved for sends.
        // For TCP, you could fill the buffer either if the remote side isn't
        // reading (so that remote buffer becomes full, then TCP communicates this
        // fact to your kernel, and your kernel stops sending data, instead
        // accumulating it in the local buffer until it fills up). Or it could
        // fill up if there is a network problem, and the kernel isn't getting
        // acknowledgements for the data it sends. It will then slow down sending data
        // on the network until, eventually, the outgoing buffer fills up.
        channelConfig.setOption(ChannelOption.SO_SNDBUF, soSndBuf);

        // Specifies the total per-socket buffer space reserved for receives.
        // The size of the buffer the kernel allocates to hold the data arriving
        // into the given socket during the time between it arrives over the network
        // and when it is read by the program that owns this socket. With TCP,
        // if data arrives and you aren't reading it, the buffer will fill up,
        // and the sender will be told to slow down (using TCP window adjustment mechanism).
        channelConfig.setOption(ChannelOption.SO_RCVBUF, soRcvBuf);

        // Limit the size of inbound buffers that are being created by netty when client sends
        // large files. I want a client to stop sending data over the network when ByteBuf is
        // full and resume once it becomes ready to receive more data.
        //channelConfig.setOption(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(<size>));

        // If the number of bytes queued in the write buffer exceeds writeBufferHighWaterMark value,
        // Channel.isWritable() will start to return false.
        channelConfig.setOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, writeBufferHighWatermark);

        // Once the number of bytes queued in the write buffer exceeded the high water mark and
        // then dropped down below this value, Channel.isWritable() will return true again.
        channelConfig.setOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, writeBufferLowWatermark);
    }
}
