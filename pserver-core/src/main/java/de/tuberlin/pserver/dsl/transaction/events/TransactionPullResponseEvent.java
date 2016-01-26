package de.tuberlin.pserver.dsl.transaction.events;


import java.util.Map;

public class TransactionPullResponseEvent extends TransactionEvent {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String TRANSACTION_RESPONSE = "transaction_response_";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final Map<String, Object> responseSrcStateObjects;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionPullResponseEvent() { this(null, null); }
    public TransactionPullResponseEvent(final String transactionName, final Map<String, Object> responseSrcStateObjects) {
        super(TRANSACTION_RESPONSE + transactionName);
        this.responseSrcStateObjects = responseSrcStateObjects;
    }
}
