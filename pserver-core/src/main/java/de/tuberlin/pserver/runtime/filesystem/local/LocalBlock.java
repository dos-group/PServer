package de.tuberlin.pserver.runtime.filesystem.local;


import de.tuberlin.pserver.runtime.filesystem.AbstractBlock;

public final class LocalBlock implements AbstractBlock {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final long offset;

    public final long linesToRead;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public LocalBlock(long offset, long linesToRead) {
        this.offset      = offset;
        this.linesToRead = linesToRead;
    }
}
