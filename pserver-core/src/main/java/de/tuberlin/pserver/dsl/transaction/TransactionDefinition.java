package de.tuberlin.pserver.dsl.transaction;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.transaction.phases.Apply;
import de.tuberlin.pserver.dsl.transaction.phases.Fuse;
import de.tuberlin.pserver.dsl.transaction.phases.Prepare;

import java.util.UUID;

public class TransactionDefinition<T> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final UUID uid = UUID.randomUUID();

    public final Prepare<T> preparePhase;

    public final Fuse<T> fusionPhase;

    public final Apply<T> applyPhase;

    private String transactionName;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionDefinition(final Apply<T> applyPhase) {

        this(null, null, applyPhase);
    }

    public TransactionDefinition(final Fuse<T> fusionPhase,
                                 final Apply<T> applyPhase) {

        this(null, fusionPhase, applyPhase);
    }

    public TransactionDefinition(final Prepare<T> preparePhase,
                                 final Apply<T> applyPhase) {

        this(preparePhase, null, applyPhase);
    }


    public TransactionDefinition(final Prepare<T> preparePhase,
                                 final Fuse<T> fusionPhase,
                                 final Apply<T> applyPhase) {

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
