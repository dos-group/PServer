package de.tuberlin.pserver.runtime.filesystem;

import de.tuberlin.pserver.runtime.core.network.NetEvent;


public final class FilePartitionEvent extends NetEvent {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public static final String FILE_PARTITION_EVENT = "file_partition_event";

    public AbstractFilePartition filePartitionDescriptor;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public FilePartitionEvent() { this(null); }
    public FilePartitionEvent(AbstractFilePartition filePartitionDescriptor) {
        super(FILE_PARTITION_EVENT);
        this.filePartitionDescriptor = filePartitionDescriptor;
    }
}
