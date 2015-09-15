package de.tuberlin.pserver.dsl.dataflow.shared;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.SlotContext;
import de.tuberlin.pserver.runtime.SlotGroup;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public final class SharedVar<T> {

    private static final ThreadLocal<MutableLong> threadLocalSharedVarCounter;

    static {

        threadLocalSharedVarCounter = new ThreadLocal<MutableLong>() {

            @Override
            protected MutableLong initialValue() {

                return new MutableLong(0);
            }
        };
    }

    // ---------------------------------------------------

    private final SlotContext sc;

    private final Pair<Integer, Long> sharedVarUID;

    private Triple<AtomicReference<T>, ReentrantLock, AtomicInteger> managedVar;

    // ---------------------------------------------------

    public SharedVar(final SlotContext sc, final T value) throws Exception {

        this.sc = Preconditions.checkNotNull(sc);

        final SlotGroup slotGroup = sc.getActiveSlotGroup();

        final int masterSlotID = slotGroup.minSlotID;

        final int refNum = slotGroup.maxSlotID - slotGroup.minSlotID + 1;

        this.sharedVarUID = nextSharedVarUID(masterSlotID);

        //sc.CF.parUnit().slot(masterSlotID).exe(() -> {

            if (sc.slotID == masterSlotID) {

                final AtomicReference<T> valueRef = new AtomicReference<>(Preconditions.checkNotNull(value));

                final ReentrantLock valueLock = new ReentrantLock(true);

                final AtomicInteger refCount = new AtomicInteger(refNum);

                final Triple<AtomicReference<T>, ReentrantLock, AtomicInteger> managedVar = Triple.of(valueRef, valueLock, refCount);

                sc.programContext.put(sharedVarUIDStr(), managedVar);
            }
        //});
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

        this.managedVar.getMiddle().lock();
    }

    public void unlock() {

        Preconditions.checkState(this.managedVar != null);

        this.managedVar.getMiddle().unlock();
    }

    public T done() throws Exception {

        int refCount = this.managedVar.getRight().decrementAndGet();

        if (refCount == 0) {

            Preconditions.checkState(this.managedVar != null);

            sc.programContext.delete(sharedVarUIDStr());
        }

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
