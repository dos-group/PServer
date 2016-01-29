package de.tuberlin.pserver.dsl.transaction;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.transaction.annotations.TransactionType;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TransactionMng {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static ProgramContext programContext;

    private static TransactionBuilder transactionBuilder;

    private static final Map<String, List<Object>> asyncTransactionResult = new HashMap<>();

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public static void setProgramContext(final ProgramContext programContext) {
        TransactionMng.programContext = Preconditions.checkNotNull(programContext);
        TransactionMng.transactionBuilder = new TransactionBuilder(TransactionMng.programContext);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static <T> T commit(final TransactionDefinition transactionDefinition) throws Exception{
        return commit(transactionDefinition, null);
    }

    @SuppressWarnings("unchecked")
    public static <T> T commit(final TransactionDefinition transactionDefinition, final Object requestObject) throws Exception {
        final String transactionName = transactionDefinition.getTransactionName();
        final TransactionController controller = programContext.programTable.getTransactionController(transactionName);
        Preconditions.checkState(controller != null);
        return (T)controller.executeTransaction(requestObject);
    }

    public static void asyncCommit(final TransactionDefinition transactionDefinition) throws Exception {
        asyncCommit(transactionDefinition, null);
    }

    public static void asyncCommit(final TransactionDefinition transactionDefinition, final Object requestObject) throws Exception {
        // Only for pull transactions.
        if (programContext.programTable.getTransaction(transactionDefinition.getTransactionName()).type == TransactionType.PULL) {
            new Thread(() -> {
                try {
                    Object result = commit(transactionDefinition);
                    synchronized (asyncTransactionResult) {
                        List<Object> txnResults = asyncTransactionResult.get(transactionDefinition.getTransactionName());
                        if (txnResults == null) {
                            txnResults = new ArrayList<>();
                            asyncTransactionResult.put(transactionDefinition.getTransactionName(), txnResults);
                        }
                        txnResults.add(result);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).start();
        } else
            commit(transactionDefinition, requestObject);
    }

    public static List<Object> getResult(final TransactionDefinition transactionDefinition) {
        return asyncTransactionResult.get(transactionDefinition.getTransactionName());
    }

    public static TransactionBuilder getTransactionBuilder() {
        transactionBuilder.clear();
        return transactionBuilder;
    }
}
