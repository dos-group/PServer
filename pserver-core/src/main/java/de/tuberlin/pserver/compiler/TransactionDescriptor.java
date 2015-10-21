package de.tuberlin.pserver.compiler;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.utils.ParseUtils;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.properties.TransactionType;
import org.apache.commons.lang3.ArrayUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public final class TransactionDescriptor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final String transactionName;

    public final List<String> stateObjectNames;

    public final TransactionDefinition definition;

    public final TransactionType type;

    public final boolean cacheRequestObject;

    public final List<int[]> stateObjectNodes;

    public final int latchCount;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionDescriptor(final String transactionName,
                                 final List<String> stateObjectNames,
                                 final TransactionDefinition definition,
                                 final TransactionType type,
                                 final boolean cacheRequestObject,
                                 final int nodeID,
                                 final ProgramTable programTable) {

        this.transactionName = Preconditions.checkNotNull(transactionName);

        this.stateObjectNames = Preconditions.checkNotNull(stateObjectNames);

        this.definition = Preconditions.checkNotNull(definition);

        this.type  = Preconditions.checkNotNull(type);

        this.cacheRequestObject = cacheRequestObject;

        this.stateObjectNodes = new ArrayList<>();

        int latchCount = 0;
        for (final String stateObjectName : stateObjectNames) {
            final int[] stateNodes = ArrayUtils.removeElements(programTable.getState(stateObjectName).atNodes, nodeID);
            stateObjectNodes.add(stateNodes);
            latchCount += stateNodes.length;
        }

        this.latchCount = latchCount;

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

        final List<String> stateObjects = ParseUtils.parseStateList(transaction.state());
        return new TransactionDescriptor(
                field.getName(),
                stateObjects,
                (TransactionDefinition)field.get(instance),
                transaction.type(),
                transaction.cache(),
                nodeID,
                programTable
        );
    }
}
