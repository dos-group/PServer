package de.tuberlin.pserver.dsl.transaction.events;



public class TransactionResponseEvent extends TransactionEvent {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String TRANSACTION_RESPONSE = "transaction_response_";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final Object responseObject;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionResponseEvent(final String transactionName, final Object responseObject) {

        super(TRANSACTION_RESPONSE + transactionName);

        this.responseObject = responseObject;
    }
}
