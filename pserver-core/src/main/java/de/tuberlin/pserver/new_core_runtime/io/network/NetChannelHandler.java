package de.tuberlin.pserver.new_core_runtime.io.network;

public interface NetChannelHandler<T> {

    public void handle(NetChannel netChannel, T msg);
}
