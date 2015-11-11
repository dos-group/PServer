package de.tuberlin.pserver.dsl.transaction.events;

import com.google.common.base.Preconditions;

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

    public final List<String> stateObjectNames;

    public final List<Object> stateObjectsValues;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionPushRequestEvent(final String transactionName,
                                   final List<String> stateObjectNames,
                                   final List<Object> stateObjectsValues,
                                   final Object requestObject,
                                   final boolean cacheRequest) {

        super(TRANSACTION_REQUEST + transactionName);

        this.stateObjectNames = Preconditions.checkNotNull(stateObjectNames);

        this.stateObjectsValues = Preconditions.checkNotNull(stateObjectsValues);

        this.requestObject = requestObject;

        this.cacheRequest = cacheRequest;
    }
}