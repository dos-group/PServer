package de.tuberlin.pserver.runtime.core.network;

public interface NetChannelHandler<T> {

    public void handle(NetChannel netChannel, T msg);
}
