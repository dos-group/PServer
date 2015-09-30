package de.tuberlin.pserver.dsl.transaction;


import com.google.common.base.Preconditions;

public final class TransactionDeclaration {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final String transactionName;

    public final String stateName;

    public final TransactionType type;

    public final int[] nodes;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionDeclaration(final String transactionName,
                                  final String stateName,
                                  final TransactionType type,
                                  final int[] nodes) {

        this.transactionName = Preconditions.checkNotNull(transactionName);

        this.stateName = Preconditions.checkNotNull(stateName);

        this.type  = Preconditions.checkNotNull(type);

        this.nodes = nodes;
    }
}
