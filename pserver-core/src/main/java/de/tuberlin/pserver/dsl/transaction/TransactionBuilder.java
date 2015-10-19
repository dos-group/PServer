package de.tuberlin.pserver.dsl.transaction;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.compiler.TransactionDescriptor;
import de.tuberlin.pserver.dsl.transaction.properties.TransactionType;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

public final class TransactionBuilder {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final ProgramContext programContext;

    // ---------------------------------------------------

    public String stateName;

    public TransactionType type;

    public String at;

    public boolean cache;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public TransactionBuilder(final ProgramContext programContext) {

        this.programContext = Preconditions.checkNotNull(programContext);

        clear();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public TransactionBuilder state(final String stateName) { this.stateName = stateName; return this; }

    public TransactionBuilder type(final TransactionType type) { this.type = type; return this; }

    public TransactionBuilder at(final String at) { this.at = at; return this; }

    public TransactionBuilder cache(final boolean cache) { this.cache = cache; return this; }

    // ---------------------------------------------------

    public TransactionDefinition build(final String transactionName, final TransactionDefinition definition) {
        final TransactionDescriptor descriptor = new TransactionDescriptor(
                transactionName,
                stateName,
                definition,
                type,
                cache,
                programContext.runtimeContext.nodeID,
                programContext.programTable.getState(stateName).atNodes
        );
        return programContext.runtimeContext.runtimeManager.createTransaction(programContext, descriptor);
    }

    // ---------------------------------------------------

    public void clear() {
        this.stateName = "";
        this.type = TransactionType.PUSH;
        this.at = "";
        this.cache = false;
    }
}
