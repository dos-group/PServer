package de.tuberlin.pserver.dsl;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.SlotContext;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public final class SharedVar<T> {

    private static final ThreadLocal<MutableLong> threadLocalSharedVarCounter;

    static {

        threadLocalSharedVarCounter = new ThreadLocal<>();

        threadLocalSharedVarCounter.set(new MutableLong(0));
    }

    // ---------------------------------------------------

    private final SlotContext sc;

    private final Pair<Integer, Long> sharedVarUID;

    private Pair<AtomicReference<T>, ReentrantLock> managedVar;

    // ---------------------------------------------------

    public SharedVar(final SlotContext sc, final T value) throws Exception {

        this.sc = Preconditions.checkNotNull(sc);

        final int slotID = sc.programContext.runtimeContext.executionManager.getAvailableSlotRangeForScope().getLeft();

        this.sharedVarUID = nextSharedVarUID(slotID);

        sc.CF.select().slot(slotID).exe(() -> {

            final AtomicReference<T> valueRef = new AtomicReference<>(Preconditions.checkNotNull(value));

            final ReentrantLock valueLock = new ReentrantLock(true);

            final Pair<AtomicReference<T>, ReentrantLock> managedVar = Pair.of(valueRef, valueLock);

            sc.programContext.put(sharedVarUIDStr(), managedVar);
        });
    }

    // ---------------------------------------------------

    public void set(final T value) {

        fetch();

        this.managedVar.getLeft().set(value);
    }

    public T get() {

        fetch();

        return this.managedVar.getLeft().get();
    }

    public void lock() {

        fetch();

        this.managedVar.getRight().lock();
    }

    public void unlock() {

        Preconditions.checkState(this.managedVar != null);

        this.managedVar.getRight().unlock();
    }

    public T done() throws Exception {

        final int slotID = sc.programContext.runtimeContext.executionManager.getAvailableSlotRangeForScope().getLeft();

        sc.CF.select().slot(slotID).exe(() -> {

            Preconditions.checkState(this.managedVar != null);

            sc.programContext.delete(sharedVarUIDStr());
        });

        return this.managedVar.getLeft().get();
    }

    public SharedVar<T> fetch() {

        while(this.managedVar == null) { // busy waiting.

            this.managedVar = sc.programContext.get(sharedVarUIDStr());
        }

        return this;
    }

    public T acquire() throws Exception {

        return fetch().done();
    }

    // ---------------------------------------------------

    private String sharedVarUIDStr() {

        return "__shared_var_" + sharedVarUID.toString() + "__";
    }

    // ---------------------------------------------------

    private static Pair<Integer, Long> nextSharedVarUID(final int allocatorSlot) {

        if (threadLocalSharedVarCounter.get().getValue() == Long.MAX_VALUE)
            threadLocalSharedVarCounter.get().setValue(0L);

        threadLocalSharedVarCounter.get().increment();

        return Pair.of(allocatorSlot, threadLocalSharedVarCounter.get().getValue());
    }
}
