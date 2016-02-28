package de.tuberlin.pserver.runtime.filesystem.local;

import de.tuberlin.pserver.runtime.filesystem.AbstractFile;
import de.tuberlin.pserver.runtime.filesystem.AbstractFileIterator;
import de.tuberlin.pserver.types.typeinfo.DistributedTypeInfo;

public class LocalFile extends AbstractFile {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final DistributedTypeInfo typeInfo;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public LocalFile(DistributedTypeInfo typeInfo) { this.typeInfo = typeInfo; }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override public DistributedTypeInfo getTypeInfo() { return typeInfo; }

    @Override public AbstractFileIterator getFileIterator() { return new LocalFileIterator(this); }
}
