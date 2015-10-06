package de.tuberlin.pserver.dsl.transaction;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.transaction.phases.Apply;
import de.tuberlin.pserver.dsl.transaction.phases.Fuse;
import de.tuberlin.pserver.dsl.transaction.phases.Prepare;


public class TransactionDefinition {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final Prepare preparePhase;

    public final Fuse fusionPhase;

    public final Apply applyPhase;

    private String transactionName;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionDefinition(final Apply applyPhase) {

        this(null, null, applyPhase);
    }

    public TransactionDefinition(final Fuse fusionPhase,
                                 final Apply applyPhase) {

        this(null, fusionPhase, applyPhase);
    }

    public TransactionDefinition(final Prepare preparePhase,
                                 final Apply applyPhase) {

        this(preparePhase, null, applyPhase);
    }


    public TransactionDefinition(final Prepare preparePhase,
                                 final Fuse fusionPhase,
                                 final Apply applyPhase) {

        this.preparePhase = preparePhase;

        this.fusionPhase  = fusionPhase;

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
