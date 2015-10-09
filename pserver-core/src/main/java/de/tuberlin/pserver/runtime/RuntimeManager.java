package de.tuberlin.pserver.runtime;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.compiler.*;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.dsl.transaction.TransactionController;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.utils.MatrixBuilder;
import de.tuberlin.pserver.runtime.dht.DHTKey;
import de.tuberlin.pserver.runtime.dht.DHTManager;
import de.tuberlin.pserver.runtime.dht.types.EmbeddedDHTObject;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.partitioning.IMatrixPartitioner;
import de.tuberlin.pserver.runtime.partitioning.MatrixPartitionManager;
import de.tuberlin.pserver.types.DistributedMatrix;
import de.tuberlin.pserver.types.RemoteMatrixSkeleton;
import de.tuberlin.pserver.types.RemoteMatrixStub;
import org.apache.commons.lang3.ArrayUtils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;
import java.util.stream.IntStream;

public final class RuntimeManager {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static enum ReceiveType {SYNC, ASYNC}

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int[] nodeIDs, remoteNodeIDs;

    // ---------------------------------------------------

    private final InfrastructureManager infraManager;

    private final NetManager netManager;

    private final FileSystemManager fileManager;

    private final MatrixPartitionManager matrixPartitionManager;

    private final DHTManager dhtManager;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public RuntimeManager(final InfrastructureManager infraManager,
                          final NetManager netManager,
                          final FileSystemManager fileManager,
                          final DHTManager dhtManager) {

        this.infraManager   = Preconditions.checkNotNull(infraManager);
        this.netManager     = Preconditions.checkNotNull(netManager);
        this.fileManager    = Preconditions.checkNotNull(fileManager);
        this.dhtManager     = Preconditions.checkNotNull(dhtManager);

        this.matrixPartitionManager = new MatrixPartitionManager(netManager, fileManager, this);

        this.nodeIDs = IntStream.iterate(0, x -> x + 1).limit(infraManager.getMachines().size()).toArray();
        this.remoteNodeIDs = ArrayUtils.removeElements(nodeIDs, infraManager.getNodeID());
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void bind(final Program instance) throws Exception {
        allocateState(instance.programContext);
        matrixPartitionManager.loadFilesIntoDHT();

        try {
            Thread.sleep(5000); // TODO: bullshit
        } catch(Exception e) {
            e.printStackTrace();
        }

        bindState(instance.programContext.programTable, instance);
        invokeProgram(instance.programContext.programTable, instance);
    }

    public void clearContext() {
        matrixPartitionManager.clearContext();
        fileManager.clearContext();
    }

    public void allocateState(final ProgramContext programContext, StateDescriptor decl) throws Exception {
        if (Matrix.class.isAssignableFrom(decl.stateType)) {
            switch (decl.scope) {
                case SINGLETON: {

                    if (decl.atNodes.length != 1)
                        throw new IllegalStateException();

                    if (decl.atNodes[0] < 0 || decl.atNodes[0] > nodeIDs.length - 1)
                        throw new IllegalStateException();

                    if (infraManager.getNodeID() == decl.atNodes[0]) {

                        final SharedObject so = new MatrixBuilder()
                                .dimension(decl.rows, decl.cols)
                                .format(decl.format)
                                .layout(decl.layout)
                                .build();

                        putDHT(decl.stateName, so);

                        new RemoteMatrixStub(programContext, decl.stateName, (Matrix)so);

                    } else {

                        final RemoteMatrixSkeleton remoteMatrixSkeleton = new RemoteMatrixSkeleton(
                                programContext,
                                decl.stateName,
                                decl.atNodes[0],
                                decl.rows,
                                decl.cols,
                                decl.format,
                                decl.layout
                        );

                        putDHT(decl.stateName, remoteMatrixSkeleton);
                    }

                } break;
                case REPLICATED: {
                    if ("".equals(decl.path)) {
                        if (ArrayUtils.contains(decl.atNodes, programContext.runtimeContext.nodeID)) {
                            putDHT(decl.stateName, MatrixBuilder.fromMatrixLoadTask(decl, programContext));
                        }
                    }
                    else {
                        matrixPartitionManager.addLoadTaskReturnFutureTarget(programContext, decl);
                    }
                } break;
                case PARTITIONED: {
                    if ("".equals(decl.path)) {
                        putDHT(decl.stateName, MatrixBuilder.fromMatrixLoadTask(decl, programContext));
                    } else {
                        matrixPartitionManager.addLoadTaskReturnFutureTarget(programContext, decl);
                    }
                } break;
                case LOGICALLY_PARTITIONED:
                    putDHT(decl.stateName, MatrixBuilder.fromMatrixLoadTask(decl, programContext));
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        } else
            throw new UnsupportedOperationException();
    }

    public TransactionDefinition createTransaction(final ProgramContext programContext, final TransactionDescriptor descriptor) {
        final TransactionController controller = new TransactionController(programContext.runtimeContext, descriptor);
        programContext.programTable.addTransactionController(controller);
        return descriptor.definition;
    }

    // ---------------------------------------------------

    private void bindState(final ProgramTable programTable, final Program instance) throws Exception {
        for (final StateDescriptor state : programTable.getState()) {
            final Field field = programTable.getProgramClass().getDeclaredField(state.stateName);
            final Object stateObj = getDHT(state.stateName);
            Preconditions.checkState(stateObj != null);
            field.set(instance, stateObj);
        }
    }

    private void invokeProgram(final ProgramTable programTable, final Program instance) {
        for (final UnitDescriptor decl : programTable.getUnits()) {
            if (ArrayUtils.contains(decl.atNodes, infraManager.getNodeID())) {
                try {
                    decl.method.invoke(instance, instance.getLifecycle());
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    private void allocateState(final ProgramContext programContext) throws Exception {
        for (final StateDescriptor decl : programContext.programTable.getState()) {
            allocateState(programContext, decl);
        }
    }

    // ---------------------------------------------------
    // Message-Passing Interface.
    // ---------------------------------------------------

    public synchronized void send(final String name, final Object value, final int[] nodeIDs) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(nodeIDs);
        final NetEvents.NetEvent event = new NetEvents.NetEvent(MsgEventHandler.MSG_EVENT_PREFIX + name, true);
        event.setPayload(value);
        netManager.sendEvent(nodeIDs, event);
    }

    public synchronized void send(final String name, final Object value) {
        Preconditions.checkNotNull(name);
        final NetEvents.NetEvent event = new NetEvents.NetEvent(MsgEventHandler.MSG_EVENT_PREFIX + name, true);
        event.setPayload(value);
        netManager.sendEvent(remoteNodeIDs, event);
    }

    @SuppressWarnings("unchecked")
    public <T> void receive(final int n, final String name, final Handler<T> handler) {
        receive(ReceiveType.SYNC, n, name, new MsgEventHandler() {
            @Override
            public void handleMsg(int srcNodeID, Object obj) {
                final T value = (T) obj;
                handler.handle(srcNodeID, value);
            }
        });
    }

    public void receive(final int n, final String name, final MsgEventHandler handler) {
        receive(ReceiveType.SYNC, n, name, handler);
    }

    public void receive(final ReceiveType type, final int n, final String name, final MsgEventHandler handler) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(handler);
        handler.setDispatcher(netManager);
        handler.setInfraManager(infraManager);
        handler.setRemoveAfterAwait(true);
        handler.initLatch(n);
        netManager.addEventListener(MsgEventHandler.MSG_EVENT_PREFIX + name, handler);
        if (type == ReceiveType.SYNC) {
            try {
                handler.getLatch().await();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    // ---------------------------------------------------
    // DHT Interface.
    // ---------------------------------------------------

    @SuppressWarnings("unchecked")
    public <T extends SharedObject> T getDHT(final String name) {
        final Set<DHTKey> keySet = dhtManager.getKey(name);
        for (final DHTKey key : keySet) {
            if (key.getPartitionDescriptor(infraManager.getNodeID()) != null) {
                return (T) ((EmbeddedDHTObject)dhtManager.get(key)[0]).object;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public <T extends SharedObject> void putDHT(final String name, final T object) {
        final EmbeddedDHTObject<T> value = new EmbeddedDHTObject<T>(object);
        final DHTKey key = dhtManager.createLocalKey(name);
        value.setValueMetadata(infraManager.getNodeID());
        dhtManager.put(key, value);
    }

    @SuppressWarnings("unchecked")
    public void removeDHT(final String name) {
        final Set<DHTKey> keySet = dhtManager.getKey(name);
        for (final DHTKey key : keySet) {
            if (key.getPartitionDescriptor(infraManager.getNodeID()) != null) {
                dhtManager.delete(key);
                break;
            }
        }
    }
}
