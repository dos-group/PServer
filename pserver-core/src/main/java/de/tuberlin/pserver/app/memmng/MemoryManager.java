package de.tuberlin.pserver.app.memmng;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.utils.IntervalTree;
import de.tuberlin.pserver.utils.UnsafeOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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

        // ---------------------------------------------------

        public MemoryLockSection(final long threadID, final int s, final int e) {
            this.threadID   = threadID;
            this.token      = new LockToken(s, e);
        }

        // ---------------------------------------------------

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

        private final MemoryArena arena;

        private final int index;

        private MemorySegment next;

        private LockToken lockToken;

        // ---------------------------------------------------

        public final byte[] buffer;

        public final int offset;

        public final int size;

        // ---------------------------------------------------

        public MemorySegment(final MemoryArena arena,
                             final int index,
                             final byte[] buffer,
                             final int offset,
                             final int size) {

            this.arena  = arena;
            this.index  = index;
            this.buffer = Preconditions.checkNotNull(buffer);
            this.offset = offset;
            this.size   = size;
            this.next   = null;
        }

        // ---------------------------------------------------

        public void setSuccessorSegment(final MemorySegment next) { this.next = Preconditions.checkNotNull(next); }

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
                segments.add(new MemorySegment(this, i, memory, i * segmentSize, segmentSize));
        }

        // ---------------------------------------------------

        public int getArenaSize() { return memory.length; }

        public int getSegmentSize() { return segmentSize; }

        public List<MemorySegment> getSegments() { return Collections.unmodifiableList(segments); }

        public MemorySegment allocSegmentBlocking() {
            MemorySegment ms;
            do {
                try {
                    ms = freeList.poll(DEFAULT_SEGMENT_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    LOG.warn("Segment Request Timeout.");
                    ms = null;
                }
            } while (ms == null);
            return ms;
        }

        public MemorySegment allocSegment() { return freeList.poll(); }

        public void freeSegment(final MemorySegment segment) { freeList.add(Preconditions.checkNotNull(segment)); }

        // ---------------------------------------------------

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

        public void releaseLock(final int s, final int e) {
            Preconditions.checkArgument(s >= 0 && e > s);
            final IntervalTree.Interval in = new IntervalTree.Interval(s, e);
            activeLocks.get(in);
            final MemoryLockSection es = activeLocks.get(new IntervalTree.Interval(s, e));
            synchronized (this) {
                activeLocks.remove(in);
            }
            es.unlock();
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

        // ---------------------------------------------------

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

        // ---------------------------------------------------

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

    private static final class MemoryObjectPtr implements Comparable<MemoryObjectPtr> {

        private final UUID uid;

        private final int offset;

        private final int size;

        private final List<MemorySegment> segments;

        // ---------------------------------------------------

        public MemoryObjectPtr(final UUID uid, final int offset, final int size, final List<MemorySegment> segments) {
            this.uid        = Preconditions.checkNotNull(uid);
            this.offset     = offset;
            this.size       = size;
            this.segments   = Preconditions.checkNotNull(segments);
        }

        // ---------------------------------------------------

        public byte[] extractObject() { throw new UnsupportedOperationException(); } // TODO: ...implement...

        // ---------------------------------------------------

        public void lockObject() { throw new UnsupportedOperationException(); } // TODO: ...implement...

        public void unlockObject() { throw new UnsupportedOperationException(); } // TODO: ...implement...

        // ---------------------------------------------------

        @Override
        public int compareTo(MemoryObjectPtr o) {
            return new Integer(offset).compareTo(o.offset);
        }
    }

    // ---------------------------------------------------

    /*public static final class LongLifeHeap {

        private Map<UUID, MemoryObjectPtr> objects;

        private final MemoryArena arena;

        // ---------------------------------------------------

        private MemorySegment currentSegment;

        private int currentSegmentOffset;

        // ---------------------------------------------------

        private final AtomicInteger fragmentationSize;

        private final ReentrantLock defragmentationLock;

        private final Condition defragmentationCondition;

        private volatile boolean isDefragmenting;

        // ---------------------------------------------------

        public LongLifeHeap(final int numSegments, final int segmentSize) {
            this.arena = new MemoryArena(numSegments, segmentSize);
            this.objects = new HashMap<>();
            this.fragmentationSize = new AtomicInteger();
            this.defragmentationLock = new ReentrantLock();
            this.defragmentationCondition = defragmentationLock.newCondition();

            if (getMemoryManager().longLifeUseDefragmentation)
                scheduleFragmentationTask();

            allocNewSegment();
        }

        // ---------------------------------------------------

        public MemoryObjectPtr getObjectPtr(final UUID uid) { return objects.get(Preconditions.checkNotNull(uid)); }

        public void store(final UUID uid, final byte[] data) {
            Preconditions.checkNotNull(uid);
            Preconditions.checkNotNull(data);
            defragmentationLock.lock();
            try {
                if (isDefragmenting) {
                    defragmentationCondition.await();
                }
                final MemoryObjectPtr objPtr;
                final List<MemorySegment> objSegments = new ArrayList<>();
                synchronized (this) {
                    if (!objects.containsKey(uid)) {
                        objPtr = new MemoryObjectPtr(uid, currentSegmentOffset, data.length, objSegments);
                        int firstSegmentSize = currentSegment.size - currentSegmentOffset;
                        int fillOffset = 0;
                        objSegments.add(currentSegment);
                        if (data.length <= firstSegmentSize) {
                            System.arraycopy(data, 0, currentSegment.buffer, currentSegmentOffset, data.length);
                            currentSegmentOffset += data.length;
                            if (data.length == firstSegmentSize)
                                allocNewSegment();
                            objects.put(objPtr.uid, objPtr);
                        } else {
                            System.arraycopy(data, 0, currentSegment.buffer, currentSegmentOffset, firstSegmentSize);
                            fillOffset += firstSegmentSize;
                            while (true) {
                                allocNewSegment();
                                objSegments.add(currentSegment);
                                int copySize = data.length - fillOffset;
                                if (copySize <= currentSegment.size) {
                                    System.arraycopy(data, fillOffset, currentSegment.buffer, currentSegmentOffset, copySize);
                                    if (copySize == 0)
                                        allocNewSegment();
                                    else
                                        currentSegmentOffset = copySize;
                                    break;
                                } else {
                                    System.arraycopy(data, fillOffset, currentSegment.buffer, currentSegmentOffset, currentSegment.size);
                                    fillOffset += currentSegment.size;
                                }
                            }
                            objects.put(objPtr.uid, objPtr);
                        }
                    } else {
                        throw new UnsupportedOperationException(); // TODO
                    }
                }
            } catch (InterruptedException ie) {
                throw new IllegalStateException(ie);
            } finally {
                defragmentationLock.unlock();
            }
        }

        public byte[] fetch(final UUID uid) {
            Preconditions.checkNotNull(uid);
            defragmentationLock.lock();
            try {
                if (isDefragmenting) {
                    defragmentationCondition.await();
                }
                final MemoryObjectPtr objPtr = objects.get(uid);
                if (objPtr == null)
                    throw new IllegalStateException();
                int fillOffset = 0;
                final byte[] data = new byte[objPtr.size];
                for (int i = 0; i < objPtr.segments.size(); ++i) {
                    final MemorySegment ms = objPtr.segments.get(i);
                    if (i == 0) {
                        if (objPtr.segments.size() == 1)
                            System.arraycopy(ms.buffer, objPtr.offset, data, fillOffset, objPtr.size);
                        else {
                            System.arraycopy(ms.buffer, objPtr.offset, data, fillOffset, ms.size - objPtr.offset);
                            fillOffset += ms.size - objPtr.offset;
                        }
                    } else if (i == objPtr.segments.size() - 1) {
                        System.arraycopy(ms.buffer, 0, data, fillOffset, data.length - fillOffset);
                        fillOffset += ms.size;
                        if (fillOffset != data.length)
                            throw new IllegalStateException();
                    } else {
                        System.arraycopy(ms.buffer, 0, data, fillOffset, ms.size);
                        fillOffset += ms.size;
                    }
                }
                return data;
            } catch (InterruptedException ie) {
                throw new IllegalStateException(ie);
            } finally {
                defragmentationLock.unlock();
            }
        }

        public void free(final UUID uid) {
            Preconditions.checkNotNull(uid);
            defragmentationLock.lock();
            try {
                if (isDefragmenting) {
                    defragmentationCondition.await();
                }
                final MemoryObjectPtr objPtr = objects.get(uid);
                if (objPtr == null)
                    throw new IllegalStateException();
                final int firstFragmentSize = objPtr.segments.get(0).size - objPtr.offset;
                final int lastFragmentSize = (objPtr.size - firstFragmentSize) % arena.getSegmentSize();
                fragmentationSize.addAndGet(firstFragmentSize + lastFragmentSize);
                if (objPtr.segments.size() > 2) {
                    for (int i = 1; i < objPtr.segments.size() - 1; ++i) {
                        objPtr.segments.get(i).free();
                    }
                }
            } catch (InterruptedException ie) {
                throw new IllegalStateException(ie);
            } finally {
                defragmentationLock.unlock();
            }
        }

        // ---------------------------------------------------

        private void allocNewSegment() {
            currentSegment = arena.allocSegmentBlocking();
            currentSegmentOffset = 0;
        }

        private void scheduleFragmentationTask() {
            new Timer(true).scheduleAtFixedRate(new TimerTask() {

                @Override
                public void run() {
                    final double fragmentationFactor = fragmentationSize.get() / arena.getArenaSize();
                    if (fragmentationFactor >= getMemoryManager().longLifeCriticalFragmentationFactor) {
                        isDefragmenting = true;
                        defragmentationLock.lock();
                        try {
                            defragmentHeap();
                            fragmentationSize.set(0);
                            defragmentationCondition.signal();
                        } finally {
                            defragmentationLock.unlock();
                        }
                    }
                }

            }, 0, getMemoryManager().longLifeFragmentationObserverPeriod);
        }

        private void defragmentHeap() {
            //final List<MemoryObjectPtr> orderedObjs = new ArrayList<>(objects.values());
            //Collections.sort(orderedObjs);
            //for (int i = 0; i < orderedObjs.size() - 1; ++i) {
            //    final MemoryObjectPtr objPtr = orderedObjs.get(i);
            //    int fillOffset = 0;
            //    final int segmentNum = objPtr.size / arena.getSegmentSize();
            //    final MemorySegment[] tmpSegments = MemoryManager.getMemoryManager().allocShortLifeBlocking(DEFAULT_MEMORY_SEGMENT_SIZE, segmentNum);
            //    MemoryManager.getMemoryManager().freeShortLifeSegments(tmpSegments);
            //}
            //throw new NotImplementedException();
            // TODO: Implement first fit decreasing algorithm for bin packing problem.
        }
    }*/

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(MemoryManager.class);

    private static final Object globalMemoryManagerMutex = new Object();

    private static final AtomicReference<MemoryManager> globalMemoryManagerInstance = new AtomicReference<>(null);

    // ---------------------------------------------------

    //private final IConfig config;

    //public final double longLifeHeapFraction;

    //public final int longLifeSegmentSize;

    //public final boolean longLifeUseDefragmentation;

    //public final long longLifeFragmentationObserverPeriod;

    //public final double longLifeCriticalFragmentationFactor;

    // Short Life Heap Configuration.
    //public final double shortLifeHeapFraction;

    // Global Settings.
    //public final long memoryObserverPeriod;

    //public final long segmentRequestTimeOut;

    // ---------------------------------------------------

    //private final long totalMemory;

    //private final AtomicLong freeMemory;

    //private final AtomicLong usedMemory;

    // ---------------------------------------------------

    //private final ShortLifeHeap shortLifeHeap;

    //private final MemoryArena longLifeArena;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MemoryManager(final IConfig config) {

        synchronized (globalMemoryManagerMutex) {
            if (!globalMemoryManagerInstance.compareAndSet(null, this))
                throw new IllegalStateException();
        }
        /*
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
        scheduleMemoryUsageObserverTask();*/
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

    /*private void scheduleMemoryUsageObserverTask() {
        new Timer(true).scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                freeMemory.set(Runtime.getRuntime().freeMemory());
                usedMemory.set(totalMemory - freeMemory.get());
            }

        }, 0, memoryObserverPeriod);
    }*/
}
