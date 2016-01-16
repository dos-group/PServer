package de.tuberlin.pserver.dsl.transaction.events;


import java.util.List;

public class TransactionPullResponseEvent extends TransactionEvent {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String TRANSACTION_RESPONSE = "transaction_response_";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final List<Object> responseObjects;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionPullResponseEvent() { this(null, null); }
    public TransactionPullResponseEvent(final String transactionName, final List<Object> responseObjects) {

        super(TRANSACTION_RESPONSE + transactionName);

        this.responseObjects = responseObjects;
    }
}
