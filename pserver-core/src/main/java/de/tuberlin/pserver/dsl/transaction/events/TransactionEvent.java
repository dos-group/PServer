package de.tuberlin.pserver.dsl.transaction.events;

import de.tuberlin.pserver.runtime.core.network.NetEvent;

public class TransactionEvent extends NetEvent {

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionEvent(final String eventName) { super(eventName); }
}
