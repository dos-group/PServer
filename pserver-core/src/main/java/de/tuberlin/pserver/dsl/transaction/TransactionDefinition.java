package de.tuberlin.pserver.dsl.transaction;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.transaction.phases.Combine;
import de.tuberlin.pserver.dsl.transaction.phases.GenericApply;
import de.tuberlin.pserver.dsl.transaction.phases.Prepare;


public class TransactionDefinition {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final Prepare preparePhase;

    public final Combine combinePhase;

    public final GenericApply applyPhase;

    private String transactionName;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionDefinition(final GenericApply applyPhase) {

        this(null, null, applyPhase);
    }

    public TransactionDefinition(final Combine combinePhase,
                                 final GenericApply applyPhase) {

        this(null, combinePhase, applyPhase);
    }

    public TransactionDefinition(final Prepare preparePhase,
                                 final GenericApply applyPhase) {

        this(preparePhase, null, applyPhase);
    }

    public TransactionDefinition(final Prepare preparePhase,
                                 final Combine combinePhase,
                                 final GenericApply applyPhase) {

        this.preparePhase = preparePhase;
        this.combinePhase = combinePhase;
        this.applyPhase   = Preconditions.checkNotNull(applyPhase);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void setTransactionName(final String transactionName) {
        this.transactionName = Preconditions.checkNotNull(transactionName);
    }

    public String getTransactionName() {
        return Preconditions.checkNotNull(transactionName);
    }
}
