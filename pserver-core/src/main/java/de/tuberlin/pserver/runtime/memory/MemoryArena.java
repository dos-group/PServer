package de.tuberlin.pserver.runtime.memory;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.ds.IntervalTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public final class MemoryArena {

    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(MemoryArena.class);

    private final byte[] memory;

    private final int segmentSize;

    private final List<MemorySegment> segments;

    private final IntervalTree<MemoryLockSection> activeLocks;

    private final BlockingQueue<MemorySegment> freeList;

    // ---------------------------------------------------

    public MemoryArena(final int numSegments) { this(numSegments, MemoryManager.DEFAULT_MEMORY_SEGMENT_SIZE); }
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
                ms = freeList.poll(MemoryManager.DEFAULT_SEGMENT_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
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
