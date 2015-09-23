package de.tuberlin.pserver.runtime;


import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import de.tuberlin.pserver.commons.hashtable.NonBlockingHashMap;
import de.tuberlin.pserver.commons.json.GsonUtils;
import de.tuberlin.pserver.core.infra.MachineDescriptor;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public final class MLProgramContext {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private transient static Gson gson = GsonUtils.createPrettyPrintAndAnnotationExclusionGson();

    @GsonUtils.Exclude
    public final RuntimeContext runtimeContext;

    public final MachineDescriptor clientMachine;

    public final UUID programID;

    public final String className;

    public final String simpleClassName;

    public final int nodeDOP;

    // ---------------------------------------------------

    @GsonUtils.Exclude
    private AtomicReference<CountDownLatch> globalSyncBarrier;

    // ---------------------------------------------------

    @GsonUtils.Exclude
    private final Map<String, Object> programStore;

    @GsonUtils.Exclude
    public final Map<Long, SlotContext> threadIDSlotCtxMap;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MLProgramContext(final RuntimeContext runtimeContext,
                            final MachineDescriptor clientMachine,
                            final UUID programID,
                            final String className,
                            final String simpleClassName,
                            final int nodeDOP) {

        this.runtimeContext     = Preconditions.checkNotNull(runtimeContext);
        this.clientMachine      = Preconditions.checkNotNull(clientMachine);
        this.programID          = Preconditions.checkNotNull(programID);
        this.className          = Preconditions.checkNotNull(className);
        this.simpleClassName    = Preconditions.checkNotNull(simpleClassName);
        this.nodeDOP            = nodeDOP;

        this.globalSyncBarrier  = new AtomicReference<>(new CountDownLatch(nodeDOP - 1));
        this.threadIDSlotCtxMap = new ConcurrentHashMap<>();
        this.programStore       = new NonBlockingHashMap<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public synchronized void addSlotContext(final long threadID, final SlotContext sc) {
        threadIDSlotCtxMap.put(threadID, Preconditions.checkNotNull(sc));
    }

    public synchronized void removeSlotContext(final long threadID) {
        threadIDSlotCtxMap.remove(threadID);
    }

    // ---------------------------------------------------

    public void put(final String name, final Object obj) { programStore.put(Preconditions.checkNotNull(name), Preconditions.checkNotNull(obj)); }

    @SuppressWarnings("unchecked")
    public <T> T get(final String name) { return (T)programStore.get(Preconditions.checkNotNull(name)); }

    public void delete(final String name) { programStore.remove(Preconditions.checkNotNull(name)); }

    // ---------------------------------------------------

    public void countDownBarrier() {
        while (globalSyncBarrier.get().getCount() == 0) { // TODO: Change this!
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        globalSyncBarrier.get().countDown();
    }

    public void awaitGlobalSyncBarrier() {
        try {
            globalSyncBarrier.get().await();
            globalSyncBarrier.set(new CountDownLatch(nodeDOP - 1));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // ---------------------------------------------------

    @Override
    public String toString() {
        return "\nMLProgramContext " + gson.toJson(this);
    }
}
