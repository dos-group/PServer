package de.tuberlin.pserver.runtime;


import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import de.tuberlin.pserver.commons.hashtable.NonBlockingHashMap;
import de.tuberlin.pserver.commons.json.GsonUtils;
import de.tuberlin.pserver.compiler.ProgramTable;
import de.tuberlin.pserver.compiler.UnitDescriptor;
import de.tuberlin.pserver.core.common.Deactivatable;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public final class ProgramContext implements Deactivatable {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String BARRIER_EVENT  = "barrier_event_";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private transient static Gson gson = GsonUtils.createPrettyPrintAndAnnotationExclusionGson();

    public final MachineDescriptor clientMachine;

    public final UUID programID;

    public final String className;

    public final String simpleClassName;

    public final int nodeDOP;

    public final String[] args;

    @GsonUtils.Exclude
    public final RuntimeContext runtimeContext;

    @GsonUtils.Exclude
    public final ProgramTable programTable;

    // ---------------------------------------------------

    @GsonUtils.Exclude
    private final Map<String, Object> programStore;

    @GsonUtils.Exclude
    private final Map<UUID, List<Serializable>> resultObjects;

    @GsonUtils.Exclude
    private final Map<String, AtomicReference<CountDownLatch>> barriers;

    @GsonUtils.Exclude
    private final Map<String, IEventHandler> handlers;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public ProgramContext(final RuntimeContext runtimeContext,
                          final MachineDescriptor clientMachine,
                          final UUID programID,
                          final String className,
                          final String simpleClassName,
                          final ProgramTable programTable,
                          final int nodeDOP,
                          final String[] args) {

        this.runtimeContext     = Preconditions.checkNotNull(runtimeContext);
        this.clientMachine      = Preconditions.checkNotNull(clientMachine);
        this.programID          = Preconditions.checkNotNull(programID);
        this.className          = Preconditions.checkNotNull(className);
        this.simpleClassName    = Preconditions.checkNotNull(simpleClassName);
        this.programTable       = Preconditions.checkNotNull(programTable);
        this.nodeDOP            = nodeDOP;
        this.args               = Preconditions.checkNotNull(args);

        this.programStore       = new NonBlockingHashMap<>();
        this.resultObjects      = new TreeMap<>();
        this.barriers           = new TreeMap<>();
        this.handlers           = new TreeMap<>();

        IEventHandler handler = event -> {
            CountDownLatch barrier = barriers.get(UnitMng.GLOBAL_BARRIER).get();
            while (barrier.getCount() == 0) {
                barrier = barriers.get(UnitMng.GLOBAL_BARRIER).get();
            }
            barrier.countDown();
        };

        barriers.put(UnitMng.GLOBAL_BARRIER, new AtomicReference<>(new CountDownLatch(nodeDOP - 1)));
        handlers.put(BARRIER_EVENT, handler);
        this.runtimeContext.netManager.addEventListener(BARRIER_EVENT, handler);

        for (final UnitDescriptor unit : programTable.getUnits()) {

            handler = event -> {
                CountDownLatch barrier = barriers.get(unit.unitName).get();
                while (barrier.getCount() == 0) { // TODO: Change this!
                    barrier = barriers.get(unit.unitName).get();
                }
                barrier.countDown();
            };

            barriers.put(unit.unitName, new AtomicReference<>(new CountDownLatch(unit.atNodes.length - 1)));
            handlers.put(BARRIER_EVENT + unit.unitName, handler);
            this.runtimeContext.netManager.addEventListener(BARRIER_EVENT + unit.unitName, handler);
        }

        UnitMng.setProgramContext(this);
        TransactionMng.setProgramContext(this);
    }

    @Override
    public void deactivate() {
        runtimeContext.netManager.removeEventListener(BARRIER_EVENT, handlers.get(BARRIER_EVENT));
        for (Map.Entry<String, IEventHandler> entry : handlers.entrySet()) {
            runtimeContext.netManager.removeEventListener(entry.getKey(), entry.getValue());
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void put(final String name, final Object obj) { programStore.put(Preconditions.checkNotNull(name), Preconditions.checkNotNull(obj)); }

    @SuppressWarnings("unchecked")
    public <T> T get(final String name) { return (T)programStore.get(Preconditions.checkNotNull(name)); }

    public void delete(final String name) { programStore.remove(Preconditions.checkNotNull(name)); }

    // ---------------------------------------------------

    public void synchronizeUnit(final String unitName) throws Exception {
        if (UnitMng.GLOBAL_BARRIER.equals(unitName)) {

            final NetEvents.NetEvent syncEvent = new NetEvents.NetEvent(BARRIER_EVENT, true);
            runtimeContext.netManager.broadcastEvent(syncEvent);
            AtomicReference<CountDownLatch> barrierRef = barriers.get(unitName);
            barrierRef.get().await();
            barrierRef.set(new CountDownLatch(nodeDOP - 1));

        } else {

            final UnitDescriptor unit = programTable.getUnit(unitName);
            final NetEvents.NetEvent syncEvent = new NetEvents.NetEvent(BARRIER_EVENT + unitName, true);
            final int[] atNodes = ArrayUtils.clone(unit.atNodes);
            final int[] receiverNodeIDs = ArrayUtils.removeElements(atNodes, runtimeContext.nodeID);
            runtimeContext.netManager.sendEvent(receiverNodeIDs, syncEvent);
            final AtomicReference<CountDownLatch> barrierRef = barriers.get(unitName);
            barrierRef.get().await();
            barrierRef.set(new CountDownLatch(programTable.getUnit(unitName).atNodes.length - 1));
        }
    }

    // ---------------------------------------------------

    public boolean node(final int fromNodeID, final int toNodeID) {
        return runtimeContext.nodeID >= fromNodeID && runtimeContext.nodeID <= toNodeID;
    }

    public boolean node(final int nodeID) { return node(nodeID, nodeID); }

    // ---------------------------------------------------

    public void setResults(final List<Serializable> results) {
        resultObjects.put(programID, Preconditions.checkNotNull(results));
    }

    public List<Serializable> getResults() {
        return resultObjects.get(programID);
    }

    // ---------------------------------------------------

    @Override
    public String toString() { return "\nMLProgramContext " + gson.toJson(this); }
}
