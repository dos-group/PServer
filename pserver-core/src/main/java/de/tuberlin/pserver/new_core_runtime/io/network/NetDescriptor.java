package de.tuberlin.pserver.new_core_runtime.io.network;

import io.netty.channel.Channel;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.*;


public final class NetDescriptor implements Serializable, Comparable<NetDescriptor> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final long serialVersionUID = -1L;

    //private transient static Gson gson = new Gson();

    public final UUID machineID;

    public final transient InetAddress address;

    public final Integer port;

    public final String hostname;

    private final transient Map<Integer, Channel> channels;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public NetDescriptor() {
        this(null, null, 0, null);
    }

    public NetDescriptor(final UUID machineID,
                         final InetAddress address,
                         final int port,
                         final String hostname) {

        //Preconditions.checkArgument(port > 1024 && port < 65535);
        this.machineID   = machineID;
        this.address     = address;
        this.port        = port;
        this.hostname    = hostname;
        this.channels    = new HashMap<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void addChannel(int channelID, Channel channel) { channels.put(channelID, channel); }

    public Channel getChannel(int channelID) { return channels.get(channelID); }

    @Override
    public int hashCode() {
        int result = machineID.hashCode();
        //result = 31 * result + address.hashCode();
        result = 31 * result + port.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other != null
                && other.getClass() == getClass()
                && machineID.equals(((NetDescriptor) other).machineID)
                && Objects.equals(port, ((NetDescriptor) other).port);
    }

    @Override
    public String toString() { return ""; } //toJson();

    @Override
    public int compareTo(final NetDescriptor o) { return machineID.compareTo(o.machineID); }
}