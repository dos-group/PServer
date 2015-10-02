package de.tuberlin.pserver.runtime;

public interface Handler<T> {

    public void handle(final int srcNodeID, final T value);
}
