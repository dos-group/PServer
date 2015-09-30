package de.tuberlin.pserver.dsl.transaction;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.compiler.ProgramCompiler;
import de.tuberlin.pserver.compiler.ProgramContext;
import de.tuberlin.pserver.dsl.state.StateDeclaration;
import de.tuberlin.pserver.dsl.state.properties.RemoteUpdate;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.dht.DHTKey;
import de.tuberlin.pserver.runtime.state.controller.RemoteUpdateController;

import java.util.List;
import java.util.StringTokenizer;

public final class TransactionMng {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static ProgramContext programContext;

    private static DataManager dataManager;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public static void setProgramContext(final ProgramContext programContext) {

        TransactionMng.programContext = Preconditions.checkNotNull(programContext);

        TransactionMng.dataManager = programContext.runtimeContext.dataManager;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static <T extends SharedObject> DHTKey put(final String name, final T obj) { return dataManager.putObject(name, obj); }

    public static <T extends SharedObject> T get(final String name) { return dataManager.getObject(name); }

    // ---------------------------------------------------
    // Old Stuff.
    // ---------------------------------------------------

    public static void publishUpdate(final String objNames) throws Exception {
        Preconditions.checkNotNull(objNames);
        final StringTokenizer st = new StringTokenizer(objNames, ",");
        while (st.hasMoreTokens()) {
            final String name = st.nextToken().replaceAll("\\s+", "");
            final RemoteUpdateController remoteUpdateController =
                    programContext.get(ProgramCompiler.remoteUpdateControllerName(name));
            remoteUpdateController.publishUpdate();
        }
    }

    public static void publishUpdate() throws Exception {
        final List<StateDeclaration> stateDecls = programContext.get(ProgramCompiler.stateDeclarationListName());
        for (final StateDeclaration stateDecl : stateDecls) {
            if (stateDecl.remoteUpdate != RemoteUpdate.NO_UPDATE) {
                publishUpdate(stateDecl.name);
            }
        }
    }

    public static void pullUpdate(final String objNames) throws Exception {
        Preconditions.checkNotNull(objNames);
        final StringTokenizer st = new StringTokenizer(objNames, ",");
        while (st.hasMoreTokens()) {
            final String name = st.nextToken().replaceAll("\\s+", "");
            final RemoteUpdateController remoteUpdateController =
                    programContext.get(ProgramCompiler.remoteUpdateControllerName(name));
            remoteUpdateController.pullUpdate();
        }
    }

    public static void pullUpdate() throws Exception {
        final List<StateDeclaration> stateDecls = programContext.get(ProgramCompiler.stateDeclarationListName());
        for (final StateDeclaration stateDecl : stateDecls) {
            if (stateDecl.remoteUpdate != RemoteUpdate.NO_UPDATE) {
                pullUpdate(stateDecl.name);
            }
        }
    }

    // ---------------------------------------------------

    @SuppressWarnings("unchecked")
    public static void commit(final TransactionDefinition transactionDefinition) throws Exception{
        final String transactionName = transactionDefinition.getTransactionName();
        final TransactionController controller = programContext.get(transactionName);
        Preconditions.checkState(controller != null);
        controller.executeTransaction();
    }
}
