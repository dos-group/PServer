package de.tuberlin.pserver.dsl.transaction.events;

import java.util.List;


public class TransactionPushRequestEvent extends TransactionEvent {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String TRANSACTION_REQUEST  = "transaction_push_request_";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final Object requestObject;

    public final boolean cacheRequest;

    public final List<String> srcStateObjectNames;

    public final List<Object> srcStateObjectsValues;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionPushRequestEvent() { this(null, null, null, null, false); }
    public TransactionPushRequestEvent(final String transactionName,
                                   final List<String> srcStateObjectNames,
                                   final List<Object> srcStateObjectsValues,
                                   final Object requestObject,
                                   final boolean cacheRequest) {

        super(TRANSACTION_REQUEST + transactionName);

        this.srcStateObjectNames = srcStateObjectNames;

        this.srcStateObjectsValues = srcStateObjectsValues;

        this.requestObject = requestObject;

        this.cacheRequest = cacheRequest;
    }
}