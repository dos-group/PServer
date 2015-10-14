package de.tuberlin.pserver.dsl.transaction.events;


public class TransactionRequestEvent extends TransactionEvent {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String TRANSACTION_REQUEST  = "transaction_request_";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final Object requestObject;

    public final boolean cacheRequest;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionRequestEvent(final String transactionName, final Object requestObject, final boolean cacheRequest) {

        super(TRANSACTION_REQUEST + transactionName);

        this.requestObject = requestObject;

        this.cacheRequest = cacheRequest;
    }
}
