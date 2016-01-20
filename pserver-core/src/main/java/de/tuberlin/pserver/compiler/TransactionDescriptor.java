package de.tuberlin.pserver.compiler;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.utils.ParseUtils;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.annotations.TransactionType;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public final class TransactionDescriptor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final String transactionName;

    public final List<String> stateSrcObjectNames;

    public final List<String> stateDstObjectNames;

    public final TransactionDefinition definition;

    public final TransactionType type;

    public final boolean cacheRequestObject;

    public final int[] srcStateObjectNodes;

    public int[] dstStateObjectNodes;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionDescriptor(final String transactionName,
                                 final List<String> stateSrcObjectNameList,
                                 final List<String> stateDstObjectNameList,
                                 final TransactionDefinition definition,
                                 final TransactionType type,
                                 final boolean cacheRequestObject,
                                 final int nodeID,
                                 final ProgramTable programTable) {

        this.transactionName     = Preconditions.checkNotNull(transactionName);
        this.stateSrcObjectNames = new ArrayList<>(new HashSet<>(Preconditions.checkNotNull(stateSrcObjectNameList)));
        this.stateDstObjectNames = new ArrayList<>(new HashSet<>(Preconditions.checkNotNull(stateDstObjectNameList)));
        this.definition          = Preconditions.checkNotNull(definition);
        this.type                = Preconditions.checkNotNull(type);
        this.cacheRequestObject  = cacheRequestObject;
        this.srcStateObjectNodes = programTable.getState(stateSrcObjectNames.get(stateSrcObjectNames.size() - 1)).atNodes;
        this.dstStateObjectNodes = programTable.getState(stateDstObjectNames.get(stateDstObjectNames.size() - 1)).atNodes;
        definition.setTransactionName(transactionName);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static TransactionDescriptor fromAnnotatedField(final Program instance,
                                                           final Transaction transaction,
                                                           final Field field,
                                                           final int nodeID,
                                                           final ProgramTable programTable) throws Exception {
        final List<String> srcObjectNames;
        final List<String> dstObjectNames;

        if (!transaction.state().equals("") &&
            transaction.src().equals("") &&
            transaction.dst().equals("")) {

            List<String> stateNames = ParseUtils.parseStateList(transaction.state());
            srcObjectNames = stateNames;
            dstObjectNames = stateNames;

        } else if (transaction.state().equals("") &&
                   !transaction.src().equals("") &&
                   !transaction.dst().equals("")) {

            srcObjectNames = ParseUtils.parseStateList(transaction.src());
            dstObjectNames = ParseUtils.parseStateList(transaction.dst());

        } else
            throw new IllegalStateException();

        return new TransactionDescriptor(
                field.getName(),
                srcObjectNames,
                dstObjectNames,
                (TransactionDefinition)field.get(instance),
                transaction.type(),
                transaction.cache(),
                nodeID,
                programTable
        );
    }
}
