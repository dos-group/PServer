package de.tuberlin.pserver.runtime.filesystem;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.types.typeinfo.DistributedTypeInfo;

public abstract class AbstractFile {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private AbstractFilePartition partition;

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void setFilePartition(AbstractFilePartition partition) {
        this.partition = Preconditions.checkNotNull(partition);
    }

    public AbstractFilePartition getFilePartition() {
        return Preconditions.checkNotNull(partition);
    }

    // ---------------------------------------------------
    // Abstract Methods.
    // ---------------------------------------------------

    public abstract DistributedTypeInfo getTypeInfo();

    public abstract AbstractFileIterator getFileIterator();
}
