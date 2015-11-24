package de.tuberlin.pserver.dsl.transaction.events;


import com.google.common.base.Preconditions;

import java.util.List;

public class TransactionPullRequestEvent extends TransactionEvent {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String TRANSACTION_REQUEST  = "transaction_request_";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final Object requestObject;

    public final boolean cacheRequest;

    public final List<String> stateObjectNames;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionPullRequestEvent(final String transactionName,
                                       final List<String> stateObjectNames,
                                       final Object requestObject,
                                       final boolean cacheRequest) {

        super(TRANSACTION_REQUEST + transactionName);

        this.stateObjectNames = Preconditions.checkNotNull(stateObjectNames);

        this.requestObject = requestObject;

        this.cacheRequest = cacheRequest;
    }
}
