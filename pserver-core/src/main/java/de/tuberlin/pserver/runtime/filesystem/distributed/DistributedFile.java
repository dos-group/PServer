package de.tuberlin.pserver.runtime.filesystem.distributed;


import de.tuberlin.pserver.commons.config.Config;
import de.tuberlin.pserver.runtime.core.network.NetManager;
import de.tuberlin.pserver.runtime.filesystem.AbstractFile;
import de.tuberlin.pserver.runtime.filesystem.AbstractFileIterator;
import de.tuberlin.pserver.types.typeinfo.DistributedTypeInfo;

public final class DistributedFile extends AbstractFile {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Config config;

    private final NetManager netManager;

    private final DistributedTypeInfo typeInfo;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedFile(Config config, NetManager netManager, DistributedTypeInfo typeInfo) {
        this.config     = config;
        this.netManager = netManager;
        this.typeInfo   = typeInfo;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override public DistributedTypeInfo getTypeInfo() { return typeInfo; }

    @Override public AbstractFileIterator getFileIterator() { return new DistributedFileIterator(config, netManager, this); }
}
