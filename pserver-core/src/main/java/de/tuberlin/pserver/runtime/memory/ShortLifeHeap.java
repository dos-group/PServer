package de.tuberlin.pserver.runtime.memory;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

public final class ShortLifeHeap {

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
