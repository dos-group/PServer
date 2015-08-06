package de.tuberlin.pserver.runtime.memory;

import com.google.common.base.Preconditions;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


public final class LongLifeHeap {

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

        if (MemoryManager.getMemoryManager().longLifeUseDefragmentation)
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
                if (fragmentationFactor >= MemoryManager.getMemoryManager().longLifeCriticalFragmentationFactor) {
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

        }, 0, MemoryManager.getMemoryManager().longLifeFragmentationObserverPeriod);
    }

    private void defragmentHeap() {
        //final List<MemoryObjectPtr> orderedObjs = new ArrayList<>(objects.values());
        //Collections.sort(orderedObjs);
        //for (int i = 0; i < orderedObjs.length() - 1; ++i) {
        //    final MemoryObjectPtr objPtr = orderedObjs.get(i);
        //    int fillOffset = 0;
        //    final int segmentNum = objPtr.length / arena.getSegmentSize();
        //    final MemorySegment[] tmpSegments = MemoryManager.getMemoryManager().allocShortLifeBlocking(DEFAULT_MEMORY_SEGMENT_SIZE, segmentNum);
        //    MemoryManager.getMemoryManager().freeShortLifeSegments(tmpSegments);
        //}
        //throw new NotImplementedException();
        // TODO: Implement first fit decreasing algorithm for bin packing problem.
    }
}
