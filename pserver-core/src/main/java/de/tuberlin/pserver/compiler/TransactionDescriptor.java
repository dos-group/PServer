package de.tuberlin.pserver.compiler;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.utils.ParseUtils;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.properties.TransactionType;

import java.lang.reflect.Field;
import java.util.Arrays;
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

    public final int[] dstStateObjectNodes;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionDescriptor(final String transactionName,
                                 final List<String> stateSrcObjectNames,
                                 final List<String> stateDstObjectNames,
                                 final TransactionDefinition definition,
                                 final TransactionType type,
                                 final boolean cacheRequestObject,
                                 final int nodeID,
                                 final ProgramTable programTable) {

        this.transactionName = Preconditions.checkNotNull(transactionName);
        this.stateSrcObjectNames = Preconditions.checkNotNull(stateSrcObjectNames);
        this.stateDstObjectNames = Preconditions.checkNotNull(stateDstObjectNames);
        this.definition = Preconditions.checkNotNull(definition);
        this.type  = Preconditions.checkNotNull(type);
        this.cacheRequestObject = cacheRequestObject;

        // Sanity check of duplicates in src name definition.
        for (int i = 0; i < stateSrcObjectNames.size() - 1; ++i) {
            if (!Arrays.equals(
                    programTable.getState(stateSrcObjectNames.get(i)).atNodes,
                    programTable.getState(stateSrcObjectNames.get(i + 1)).atNodes))
                throw new IllegalStateException();
        }

        this.srcStateObjectNodes = programTable.getState(stateSrcObjectNames.get(stateSrcObjectNames.size() - 1)).atNodes;

        //this.srcStateObjectNodes = ArrayUtils.removeElements(
        //        programTable.getState(stateSrcObjectNames.get(stateSrcObjectNames.size() - 1)).atNodes,
        //        nodeID
        //);

        // Sanity check of duplicates in dst name definition.
        for (int i = 0; i < stateDstObjectNames.size() - 1; ++i) {
            if (!Arrays.equals(
                    programTable.getState(stateDstObjectNames.get(i)).atNodes,
                    programTable.getState(stateDstObjectNames.get(i + 1)).atNodes))
                throw new IllegalStateException();
        }

        this.dstStateObjectNodes = programTable.getState(stateDstObjectNames.get(stateDstObjectNames.size() - 1)).atNodes;

                //this.dstStateObjectNodes = ArrayUtils.removeElements(
        //        programTable.getState(stateDstObjectNames.get(stateDstObjectNames.size() - 1)).atNodes,
        //        nodeID
        //);

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
