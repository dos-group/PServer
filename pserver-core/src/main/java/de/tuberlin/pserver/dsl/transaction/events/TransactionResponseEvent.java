package de.tuberlin.pserver.dsl.transaction.events;


import java.util.List;

public class TransactionResponseEvent extends TransactionEvent {

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

    public TransactionResponseEvent(final String transactionName, final List<Object> responseObjects) {

        super(TRANSACTION_RESPONSE + transactionName);

        this.responseObjects = responseObjects;
    }
}
