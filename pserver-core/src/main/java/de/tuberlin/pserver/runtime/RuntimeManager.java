package de.tuberlin.pserver.runtime;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.compiler.TransactionDescriptor;
import de.tuberlin.pserver.dsl.transaction.TransactionController;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.runtime.core.common.Deactivatable;
import de.tuberlin.pserver.runtime.core.infra.InfrastructureManager;
import de.tuberlin.pserver.runtime.core.network.NetEvent;
import de.tuberlin.pserver.runtime.core.network.NetManager;
import de.tuberlin.pserver.runtime.dht.DHTKey;
import de.tuberlin.pserver.runtime.dht.DHTManager;
import de.tuberlin.pserver.runtime.dht.types.EmbeddedDHTObject;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.events.Handler;
import de.tuberlin.pserver.runtime.events.MsgEventHandler;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.types.typeinfo.DistributedTypeInfo;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public final class RuntimeManager implements Deactivatable {

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

        this.nodeIDs        = IntStream.iterate(0, x -> x + 1).limit(infraManager.getMachines().size()).toArray();
        this.remoteNodeIDs  = ArrayUtils.removeElements(nodeIDs, infraManager.getNodeID());
    }

    public void clearContext() {
        dhtManager.clearContext();
        fileManager.clearContext();
    }

    public void deactivate() {
        dhtManager.deactivate();
        fileManager.deactivate();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public int[] getNodeIDs() { return nodeIDs; }

    public int[] getRemoteNodeIDs() { return remoteNodeIDs; }

    // ---------------------------------------------------

    public TransactionDefinition createTransaction(final ProgramContext programContext, final TransactionDescriptor descriptor) {
        final TransactionController controller = new TransactionController(programContext.runtimeContext, descriptor);
        programContext.programTable.addTransactionController(controller);
        return descriptor.definition;
    }

    // ---------------------------------------------------
    // Message-Passing Interface.
    // ---------------------------------------------------

    public void addMsgEventListener(final String name, final MsgEventHandler handler) {
        addMsgEventListener(remoteNodeIDs.length, name, handler);
    }

    public void addMsgEventListener(final int n, final String name, final MsgEventHandler handler) {
        handler.setInfraManager(infraManager);
        handler.initLatch(n);
        netManager.addEventListener(MsgEventHandler.MSG_EVENT_PREFIX + name, handler);
    }

    public void removeMsgEventListener(final String name, final MsgEventHandler handler) {
        netManager.removeEventListener(MsgEventHandler.MSG_EVENT_PREFIX + name, handler);
    }
    
    public synchronized void send(final String name, final Object value, final int[] nodeIDs) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(nodeIDs);
        final NetEvent event = new NetEvent(MsgEventHandler.MSG_EVENT_PREFIX + name, true);
        event.setPayload(value);
        netManager.dispatchEventAt(nodeIDs, event);
    }

    public synchronized void send(final String name, final Object value) {
        Preconditions.checkNotNull(name);
        final NetEvent event = new NetEvent(MsgEventHandler.MSG_EVENT_PREFIX + name, true);
        event.setPayload(value);
        netManager.dispatchEventAt(remoteNodeIDs, event);
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

    public static interface PullHandler {
        public abstract Object handlePull(final String name, final Object requestParam);
    }

    public void registerPullHandler(final String name, final PullHandler handler) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(handler);
        netManager.addEventListener(MsgEventHandler.MSG_EVENT_PREFIX + MsgEventHandler.MSG_REQUEST_EVENT_PREFIX + name, e -> {
            final NetEvent event = (NetEvent) e;
            final int srcNodeID = infraManager.getNodeIDFromMachineUID(event.srcMachineID);
            final Object result = handler.handlePull(name, event.getPayload());
            RuntimeManager.this.send(MsgEventHandler.MSG_RESPONSE_EVENT_PREFIX + name, result, new int[]{srcNodeID});
        });
    }

    public Object[] pull(final String name, Object requestParam) { return pull(name, requestParam, remoteNodeIDs); }
    public Object[] pull(final String name, Object requestParam, final int[] nodeIDs) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(nodeIDs);
        final Object[] pullResponses = new Object[nodeIDs.length];
        final AtomicInteger responseCounter = new AtomicInteger(0);
        final MsgEventHandler responseHandler = new MsgEventHandler() {
            @Override
            public void handleMsg(int srcNodeID, final Object value) {
                pullResponses[responseCounter.getAndIncrement()] = value;
            }
        };
        responseHandler.setDispatcher(netManager);
        responseHandler.setInfraManager(infraManager);
        responseHandler.setRemoveAfterAwait(true);
        responseHandler.initLatch(nodeIDs.length);
        netManager.addEventListener(MsgEventHandler.MSG_EVENT_PREFIX + MsgEventHandler.MSG_RESPONSE_EVENT_PREFIX + name, responseHandler);
        NetEvent event = new NetEvent(MsgEventHandler.MSG_EVENT_PREFIX + MsgEventHandler.MSG_REQUEST_EVENT_PREFIX + name, true);
        event.setPayload(requestParam);
        netManager.dispatchEventAt(nodeIDs, event);
        try {
            responseHandler.getLatch().await();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        return pullResponses;
    }

    // ---------------------------------------------------
    // DHT Interface.
    // ---------------------------------------------------

    @SuppressWarnings("unchecked")
    public <T extends DistributedTypeInfo> T getDHT(final String name) {
        final Set<DHTKey> keySet = dhtManager.getKey(name);
        for (final DHTKey key : keySet) {
            if (key.getPartitionDescriptor(infraManager.getNodeID()) != null) {
                return (T) ((EmbeddedDHTObject)dhtManager.get(key)[0]).object;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public <T extends DistributedTypeInfo> void putDHT(final String name, final T object) {
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

    // ----------------- PULL PRIMITIVE ------------------

    /*public static interface PullHandler {
        public abstract Object handlePull(final String name, Object requestParam);
    }

    public void registerPullHandler(final String name, final PullHandler handler) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(handler);
        netManager.addEventListener(MsgEventHandler.MSG_EVENT_PREFIX + name, e -> {
            final NetEvents.NetEvent event = (NetEvents.NetEvent) e;
            final int srcNodeID = infraManager.getNodeIDFromMachineUID(event.srcMachineID);
            final Object result = handler.handlePull(name);
            RuntimeManager.this.send(name, result, new int[]{srcNodeID});
        });
    }

    public Object[] pull(final String name) { return pull(name, remoteNodeIDs); }
    public Object[] pull(final String name, final int[] nodeIDs) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(nodeIDs);
        final Object[] pullResponses = new Object[nodeIDs.length];
        final AtomicInteger responseCounter = new AtomicInteger(0);
        final MsgEventHandler responseHandler = new MsgEventHandler() {
            @Override
            public void handleMsg(int srcNodeID, final Object value) {
                pullResponses[responseCounter.getAndIncrement()] = value;
            }
        };
        responseHandler.setDispatcher(netManager);
        responseHandler.setInfraManager(infraManager);
        responseHandler.setRemoveAfterAwait(true);
        responseHandler.initLatch(nodeIDs.length);
        netManager.addEventListener(MsgEventHandler.MSG_EVENT_PREFIX + name, responseHandler);
        NetEvents.NetEvent event = new NetEvents.NetEvent(MsgEventHandler.MSG_EVENT_PREFIX + name, true);
        netManager.sendEvent(nodeIDs, event);
        try {
            responseHandler.getLatch().await();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        return pullResponses;
    }*/
}
