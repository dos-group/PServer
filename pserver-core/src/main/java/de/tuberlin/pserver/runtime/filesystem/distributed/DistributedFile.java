package de.tuberlin.pserver.runtime.filesystem.distributed;


import de.tuberlin.pserver.runtime.filesystem.AbstractFile;
import de.tuberlin.pserver.runtime.filesystem.AbstractFileIterator;
import de.tuberlin.pserver.types.typeinfo.DistributedTypeInfo;

public final class DistributedFile extends AbstractFile {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final DistributedTypeInfo typeInfo;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedFile(DistributedTypeInfo typeInfo) { this.typeInfo = typeInfo; }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override public DistributedTypeInfo getTypeInfo() { return typeInfo; }

    @Override public AbstractFileIterator getFileIterator() { return new DistributedFileIterator(this); }
}
