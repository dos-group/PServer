package de.tuberlin.pserver.app.memmng;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.utils.UnsafeOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public final class MemoryManager {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final long    DEFAULT_MEMORY_OBSERVER_TASK_PERIOD = 1000 * 60; // 60s.

    public static final long    DEFAULT_FRAGMENTATION_OBSERVER_TASK_PERIOD = 1000 * 60 * 5; // 60s.

    public static final double  DEFAULT_CRITICAL_FRAGMENTATION_FACTOR = 0.4;

    public static final int     DEFAULT_MEMORY_SEGMENT_SIZE = 1024 * 4; // 4K.

    public static final long    DEFAULT_SEGMENT_REQUEST_TIMEOUT = 1000 * 20; // 20s

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(MemoryManager.class);

    private static final Object globalMemoryManagerMutex = new Object();

    private static final AtomicReference<MemoryManager> globalMemoryManagerInstance = new AtomicReference<>(null);

    // ---------------------------------------------------

    private final IConfig config;

    public final double longLifeHeapFraction;

    public final int longLifeSegmentSize;

    public final boolean longLifeUseDefragmentation;

    public final long longLifeFragmentationObserverPeriod;

    public final double longLifeCriticalFragmentationFactor;

    // Short Life Heap Configuration.
    public final double shortLifeHeapFraction;

    // Global Settings.
    public final long memoryObserverPeriod;

    public final long segmentRequestTimeOut;

    // ---------------------------------------------------

    private final long totalMemory;

    private final AtomicLong freeMemory;

    private final AtomicLong usedMemory;

    // ---------------------------------------------------

    private final ShortLifeHeap shortLifeHeap;

    private final MemoryArena longLifeArena;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MemoryManager(final IConfig config) {

        synchronized (globalMemoryManagerMutex) {
            if (!globalMemoryManagerInstance.compareAndSet(null, this))
                throw new IllegalStateException();
        }

        this.config = config;
        this.longLifeHeapFraction = config.getDouble("mem.longLifeHeapFraction");
        this.longLifeSegmentSize = config.getInt("mem.longLifeSegmentSize");
        this.longLifeUseDefragmentation = config.getBoolean("mem.longLifeUseDefragmentation");
        this.longLifeFragmentationObserverPeriod = config.getLong("mem.longLifeFragmentationObserverPeriod");
        this.longLifeCriticalFragmentationFactor = config.getDouble("mem.longLifeCriticalFragmentationFactor");
        this.shortLifeHeapFraction = config.getDouble("mem.shortLifeHeapFraction");
        this.memoryObserverPeriod = config.getLong("mem.longLifeFragmentationObserverPeriod");
        this.segmentRequestTimeOut = config.getLong("mem.segmentRequestTimeOut");

        this.totalMemory = Runtime.getRuntime().totalMemory();
        this.freeMemory = new AtomicLong();
        this.usedMemory = new AtomicLong();
        final int numLongLifeSegments = (int) (totalMemory / longLifeSegmentSize);
        this.longLifeArena = new MemoryArena(numLongLifeSegments, longLifeSegmentSize);
        // TODO: make dependent from <code>shortLifeHeapFraction</code>
        this.shortLifeHeap = new ShortLifeHeap(100, 10, 15);
        scheduleMemoryUsageObserverTask();
    }

    public static MemoryManager getMemoryManager() {
        return Preconditions.checkNotNull(globalMemoryManagerInstance.get());
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /*public long getTotalMemory() { return totalMemory; }

    public long getFreeMemory() { return freeMemory.get(); }

    public long getUsedMemory() { return usedMemory.get(); }

    public double getLongFileHeapFraction() { return longLifeHeapFraction; }

    public double getShortLifeHeapFraction() { return shortLifeHeapFraction; }

    // ---------------------------------------------------

    public MemorySegment allocShortLife(final int size) { return shortLifeHeap.alloc(size); }

    public MemorySegment allocShortLifeBlocking(final int size) { return shortLifeHeap.allocBlocking(size); }

    public MemorySegment[] allocShortLifeBlocking(final int size, final int num) {
        final MemorySegment[] segments = new MemorySegment[num];
        for (int i = 0; i < num; ++i)
            segments[i] = shortLifeHeap.allocBlocking(size);
        return segments;
    }

    public void freeShortLifeSegments(final MemorySegment[] segments) {
        Preconditions.checkNotNull(segments);
        for (final MemorySegment ms : segments)
            ms.free();
    }

    // ---------------------------------------------------

    public int getSegmentSize() { return longLifeSegmentSize; }

    public MemorySegment allocLongLifeSegment() { return longLifeArena.allocSegment(); }

    public Pair<Object,MemorySegment> allocSegmentAs(final Class<?> primitiveArrayClazz) {
        final MemorySegment ms = new MemorySegment(null, 0, new byte[4096], 0, longLifeSegmentSize);
        return Pair.of((Object) UnsafeOp.primitiveArrayTypeCast(ms.buffer, byte[].class, primitiveArrayClazz), ms);
    }*/

    public Object allocSegmentAs(final Class<?> primitiveArrayClazz) {
        return UnsafeOp.primitiveArrayTypeCast(new byte[4096], byte[].class, primitiveArrayClazz);
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void scheduleMemoryUsageObserverTask() {
        new Timer(true).scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                freeMemory.set(Runtime.getRuntime().freeMemory());
                usedMemory.set(totalMemory - freeMemory.get());
            }

        }, 0, memoryObserverPeriod);
    }
}
