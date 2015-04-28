package de.tuberlin.pserver.app.memmng;

import com.google.common.base.Preconditions;

public final class MemorySegment {

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
