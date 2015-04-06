package de.tuberlin.pserver.app.memmng;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.utils.IntervalTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public final class MemoryManager {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final long MEMORY_OBSERVER_TASK_PERIOD = 1000 * 60; // 60s.

    public static final int DEFAULT_MEMORY_SEGMENT_SIZE = 1024 * 4; // 4K.

    public static final long SEGMENT_REQUEST_TIMEOUT = 1000 * 20; // 20s

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private static final class LockToken {
        public final IntervalTree.Interval in;
        public LockToken(final int s, final int e) { this.in = new IntervalTree.Interval(s, e); }
    }

    // ---------------------------------------------------

    private static final class MemoryLockSection {

        public final long threadID;

        private final LockToken token;

        private ReentrantLock mutex;

        public MemoryLockSection(final long threadID, final int s, final int e) {
            this.threadID   = threadID;
            this.token      = new LockToken(s, e);
        }

        public void lock() { Preconditions.checkNotNull(mutex).lock(); }

        public void unlock() { Preconditions.checkNotNull(mutex).unlock(); mutex = null; }

        public void createMutex() { mutex = new ReentrantLock(); }

        public void transferMutex(final MemoryLockSection section) {
            Preconditions.checkNotNull(section);
            Preconditions.checkState(this.mutex == null);
            this.mutex = Preconditions.checkNotNull(section.mutex);
        }
    }

    // ---------------------------------------------------

    public static final class MemorySegment {

        public final int offset;

        public final int size;

        public final byte[] buffer;

        private MemoryArena arena;

        private MemorySegment next;

        private LockToken lockToken;

        public MemorySegment(final MemoryArena arena, final byte[] buffer, final int offset, final int size) {
            this.arena  = Preconditions.checkNotNull(arena);
            this.buffer = Preconditions.checkNotNull(buffer);
            this.offset = offset;
            this.size   = size;
            this.next   = null;
        }

        public void setNext(final MemorySegment next) { this.next = Preconditions.checkNotNull(next); }

        public MemorySegment getNext() { return next; }

        public MemoryArena getArena() { return arena; }

        public void free() { arena.freeSegment(this); }

        public void lockSegment() {
            Preconditions.checkState(lockToken == null);
            lockToken = arena.acquireLock(offset, offset + size);
        }

        public void unlockSegment() {
            Preconditions.checkState(lockToken != null);
            arena.releaseLock(lockToken);
        }
    }

    // ---------------------------------------------------

    private static final class MemoryArena {

        private final byte[] memory;

        private final int segmentSize;

        private final List<MemorySegment> segments;

        private final IntervalTree<MemoryLockSection> activeLocks;

        private final BlockingQueue<MemorySegment> freeList;

        // ---------------------------------------------------

        public MemoryArena(final int numSegments) { this(numSegments, DEFAULT_MEMORY_SEGMENT_SIZE); }
        public MemoryArena(final int numSegments, final int segmentSize) {
            this.memory         = new byte[numSegments * segmentSize];
            this.segmentSize    = segmentSize;
            this.segments       = new ArrayList<>();
            this.activeLocks    = new IntervalTree<>();
            this.freeList       = new LinkedBlockingQueue<>(segments);

            for (int i = 0; i < numSegments; ++i)
                segments.add(new MemorySegment(this, memory, i * segmentSize, segmentSize));
        }

        // ---------------------------------------------------

        public int getSegmentSize() { return segmentSize; }

        public MemorySegment allocSegmentBlocking() {
            MemorySegment ms;
            do {
                try {
                    ms = freeList.poll(SEGMENT_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    LOG.warn("Segment Request Timeout.");
                    ms = null;
                }
            } while (ms == null);
            return ms;
        }

        public MemorySegment allocSegment() { return freeList.poll(); }

        public void freeSegment(final MemorySegment segment) { freeList.add(Preconditions.checkNotNull(segment)); }

        public void copy(final MemorySegment msSrc, final int srcOffset,
                         final MemorySegment msDst, final int dstOffset,
                         final int length) {

            Preconditions.checkNotNull(msSrc);
            Preconditions.checkNotNull(msDst);
            Preconditions.checkArgument(srcOffset <= memory.length);
            Preconditions.checkArgument(dstOffset <= memory.length);
            Preconditions.checkArgument(length <= memory.length);

            System.arraycopy(msSrc.buffer, srcOffset, msDst.buffer, dstOffset, length);
        }

        public void lockCopy(final MemorySegment msSrc, final int srcOffset,
                             final MemorySegment msDst, final int dstOffset,
                             final int length) {

            final LockToken l1 = acquireLock(srcOffset, srcOffset + length);
            final LockToken l2 = acquireLock(dstOffset, dstOffset + length);
            copy(msSrc, srcOffset, msDst, dstOffset, length);
            releaseLock(l2);
            releaseLock(l1);
        }

        // ---------------------------------------------------

        public LockToken acquireLock(final int s, final int e) {
            Preconditions.checkArgument(s >= 0 && e > s);
            final long threadID = Thread.currentThread().getId();
            final MemoryLockSection ns = new MemoryLockSection(threadID, s, e);
            final IntervalTree.Interval in = activeLocks.search(ns.token.in);
            if (in != null)
                ns.transferMutex(activeLocks.get(in));
            else
                ns.createMutex();
            synchronized (this) {
                activeLocks.put(ns.token.in, ns);
            }
            ns.lock();
            return ns.token;
        }

        public void releaseLock(final LockToken token) {
            Preconditions.checkNotNull(token);
            final MemoryLockSection es = activeLocks.get(token.in);
            synchronized (this) {
                activeLocks.remove(token.in);
            }
            es.unlock();
        }
    }

    // ---------------------------------------------------

    private static final class ShortLifeHeap {

        private final Map<Integer, MemoryArena> arenaMap;

        private final int numSegmentsPerArena;

        private final int maxSegmentSize;

        public ShortLifeHeap(final int numSegmentsPerArena, final int n1, final int n2) {
            Preconditions.checkArgument(n1 >= 0 && n2 > n1);

            this.arenaMap = new HashMap<>();
            this.numSegmentsPerArena = numSegmentsPerArena;
            this.maxSegmentSize = (1 << (n2 - 1));

            for (int i = n1; i < n1 + (n2 - n1); ++i) {
                final int segmentSize = (1 << (i - 1));
                final MemoryArena ma = new MemoryArena(numSegmentsPerArena, segmentSize);
                arenaMap.put(segmentSize, ma);
            }
        }

        public int getNumSegmentsPerSize() { return numSegmentsPerArena; }

        public MemorySegment alloc(final int size) {
            Preconditions.checkArgument(size > 0);
            final int segmentSize = (Integer.SIZE * 8) - Integer.numberOfLeadingZeros(size - 1);
            return arenaMap.get(segmentSize).allocSegment();
        }

        public MemorySegment allocBlocking(final int size) {
            Preconditions.checkArgument(size > 0);
            final int segmentSize = (Integer.SIZE * 8) - Integer.numberOfLeadingZeros(size - 1);
            return arenaMap.get(segmentSize).allocSegmentBlocking();
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(MemoryManager.class);

    private static final Object globalMemoryManagerMutex = new Object();

    private static MemoryManager globalMemoryManagerInstance = null;

    // ---------------------------------------------------

    private final long totalMemory;

    private final AtomicLong freeMemory;

    private final AtomicLong usedMemory;

    private final double longLifeHeapFraction;

    private final double shortLifeHeapFraction;

    private final ShortLifeHeap shortLifeHeap;

    private final MemoryArena longLifeArena;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MemoryManager(final double longLifeHeapFraction,
                         final double shortLifeHeapFraction,
                         final int longLifeSegmentSize) {

        synchronized (globalMemoryManagerMutex) {
            if (globalMemoryManagerInstance == null)
                globalMemoryManagerInstance = this;
            else {
                throw new IllegalStateException();
            }
        }

        this.totalMemory = Runtime.getRuntime().totalMemory();
        this.freeMemory = new AtomicLong();
        this.usedMemory = new AtomicLong();
        this.longLifeHeapFraction = longLifeHeapFraction;
        this.shortLifeHeapFraction = shortLifeHeapFraction;
        final int numLongLifeSegments = (int) (totalMemory / longLifeSegmentSize);
        this.longLifeArena = new MemoryArena(numLongLifeSegments, longLifeSegmentSize);
        // TODO: make dependent from <code>shortLifeHeapFraction</code>
        this.shortLifeHeap = new ShortLifeHeap(100, 10, 15);
        scheduleMemoryUsageObserverTask();
    }

    public static MemoryManager getMemoryManagerInstance() { return globalMemoryManagerInstance; }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public long getTotalMemory() { return totalMemory; }

    public long getFreeMemory() { return freeMemory.get(); }

    public long getUsedMemory() { return usedMemory.get(); }

    public double getLongFileHeapFraction() { return longLifeHeapFraction; }

    public double getShortLifeHeapFraction() { return shortLifeHeapFraction; }

    // ---------------------------------------------------

    public MemorySegment allocShortLife(final int size) { return shortLifeHeap.alloc(size); }

    public MemorySegment allocShortLifeBlocking(final int size) { return shortLifeHeap.allocBlocking(size); }

    // ---------------------------------------------------

    public MemorySegment allocLongLifeSegment() { return longLifeArena.allocSegment(); }

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

        }, 0, MEMORY_OBSERVER_TASK_PERIOD);
    }
}
