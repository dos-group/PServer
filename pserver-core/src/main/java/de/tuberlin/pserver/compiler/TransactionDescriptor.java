package de.tuberlin.pserver.compiler;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.properties.TransactionType;
import org.apache.commons.lang3.ArrayUtils;

import java.lang.reflect.Field;

public final class TransactionDescriptor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final String transactionName;

    public final String stateName;

    public final TransactionDefinition definition;

    public final TransactionType type;

    public final boolean cacheRequestObject;

    public final int[] nodes;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionDescriptor(final String transactionName,
                                 final String stateName,
                                 final TransactionDefinition definition,
                                 final TransactionType type,
                                 final boolean cacheRequestObject,
                                 final int nodeID,
                                 final int[] nodes) {

        this.transactionName = Preconditions.checkNotNull(transactionName);

        this.stateName = Preconditions.checkNotNull(stateName);

        this.definition = Preconditions.checkNotNull(definition);

        this.type  = Preconditions.checkNotNull(type);

        this.cacheRequestObject = cacheRequestObject;

        this.nodes = ArrayUtils.removeElements(nodes, nodeID);

        definition.setTransactionName(transactionName);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static TransactionDescriptor fromAnnotatedField(final Program instance,
                                                           final Transaction transaction,
                                                           final Field field,
                                                           final int nodeID,
                                                           final int[] atNodes) throws Exception {
        return new TransactionDescriptor(
                field.getName(),
                transaction.state(),
                (TransactionDefinition)field.get(instance),
                transaction.type(),
                transaction.cache(),
                nodeID,
                atNodes
        );
    }
}
