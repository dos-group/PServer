package de.tuberlin.pserver.compiler;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.utils.ParseUtils;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.properties.TransactionType;
import org.apache.commons.lang3.ArrayUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
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

    public final int[] stateObjectNodes;

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

        for (int i = 0; i < stateObjectNames.size() - 1; ++i) {
            //final int[] stateNodes = ArrayUtils.removeElements(programTable.getState(stateObjectName).atNodes, nodeID);

            if (!Arrays.equals(
                    programTable.getState(stateObjectNames.get(i)).atNodes,
                    programTable.getState(stateObjectNames.get(i + 1)).atNodes))
                throw new IllegalStateException();
        }

        this.stateObjectNodes = ArrayUtils.removeElements(
                programTable.getState(stateObjectNames.get(stateObjectNames.size() - 1)).atNodes,
                nodeID
        );

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
