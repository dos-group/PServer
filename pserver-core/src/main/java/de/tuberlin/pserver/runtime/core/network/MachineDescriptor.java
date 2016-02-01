package de.tuberlin.pserver.runtime.core.network;

import com.google.gson.Gson;
import io.netty.channel.Channel;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class MachineDescriptor implements Serializable, Comparable<MachineDescriptor> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final long serialVersionUID = -1L;

    private transient static Gson gson = new Gson();

    public final UUID machineID;

    public final transient InetAddress address;

    public final Integer port;

    public final String hostname;

    private final transient Map<Integer, Channel> channels;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MachineDescriptor() { this(null, null, 0, null); }
    public MachineDescriptor(final UUID machineID,
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

    public void removeChannel(int channelID) { channels.remove(channelID); }

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
                && machineID.equals(((MachineDescriptor) other).machineID)
                && Objects.equals(port, ((MachineDescriptor) other).port);
    }

    @Override
    public String toString() { return ""; } //toJson();

    @Override
    public int compareTo(final MachineDescriptor o) { return machineID.compareTo(o.machineID); }

    public String toJson() { return gson.toJson(this); }
}