package de.tuberlin.pserver.new_core_runtime.io.network;

import de.tuberlin.pserver.new_core_runtime.events.Event;

public class NetChannelEvent extends Event {

    public final NetChannel netChannel;

    public NetChannelEvent(NetChannel netChannel, Object msg) {
        super(msg.getClass().getName(), msg);
        this.netChannel = netChannel;
    }
}