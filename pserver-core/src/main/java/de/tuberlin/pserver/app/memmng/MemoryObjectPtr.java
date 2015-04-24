package de.tuberlin.pserver.app.memmng;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.UUID;

final class MemoryObjectPtr implements Comparable<MemoryObjectPtr> {

    public final UUID uid;

    public final int offset;

    public final int size;

    public final List<MemorySegment> segments;

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
    public int compareTo(MemoryObjectPtr o) { return new Integer(offset).compareTo(o.offset); }
}
