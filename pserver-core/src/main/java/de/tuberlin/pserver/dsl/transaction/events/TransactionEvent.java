package de.tuberlin.pserver.dsl.transaction.events;


import de.tuberlin.pserver.runtime.core.net.NetEvents;

public class TransactionEvent extends NetEvents.NetEvent {

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionEvent(final String eventName) { super(eventName); }
}
