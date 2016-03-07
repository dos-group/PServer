package de.tuberlin.pserver.runtime.filesystem.local;

import de.tuberlin.pserver.runtime.filesystem.AbstractFilePartition;
import de.tuberlin.pserver.types.typeinfo.properties.FileFormat;


public class LocalFilePartition extends AbstractFilePartition {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public LocalBlock localBlock;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public LocalFilePartition() { this(-1, null, null, -1, -1); }
    public LocalFilePartition(int nodeID, String file, FileFormat fileFormat, long offset, long linesToRead) {
        super(nodeID, file, fileFormat);
        this.localBlock = new LocalBlock(offset, linesToRead);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    //@Override public String toString() { return "\nLocalFilePartition " + gson.toJson(this); }
}
