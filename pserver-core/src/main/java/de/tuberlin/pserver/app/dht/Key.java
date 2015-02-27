package de.tuberlin.pserver.app.dht;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.infra.MachineDescriptor;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class Key implements Serializable, Comparable<Key> {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static final class PartitionDescriptor implements Serializable {

        public final int partitionIndex;

        public final int partitionSize;

        public final int globalOffset;

        public final int segmentBaseIndex;

        public final int numberOfSegments;

        public final int segmentSize;

        public final MachineDescriptor machine;

        public PartitionDescriptor(final int partitionIndex,
                                   final int partitionSize,
                                   final int globalOffset,
                                   final int segmentBaseIndex,
                                   final int numberOfSegments,
                                   final int segmentSize,
                                   final MachineDescriptor machine) {

            Preconditions.checkArgument(partitionIndex >= 0);
            Preconditions.checkArgument(partitionSize > 0);
            Preconditions.checkArgument(globalOffset >= 0);
            Preconditions.checkArgument(segmentBaseIndex >= 0);
            Preconditions.checkArgument(numberOfSegments > 0);
            Preconditions.checkArgument(segmentSize >= 0 && segmentSize % DEFAULT_ALIGNMENT_SIZE == 0);

            this.partitionIndex = partitionIndex;
            this.partitionSize = partitionSize;
            this.globalOffset = globalOffset;
            this.segmentBaseIndex = segmentBaseIndex;
            this.numberOfSegments = numberOfSegments;
            this.segmentSize = segmentSize;
            this.machine = Preconditions.checkNotNull(machine);
        }
    }

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final long serialVersionUID = -1;

    public static final int DEFAULT_ALIGNMENT_SIZE = 8; // (in bytes)

    public static final int DEFAULT_SEGMENT_SIZE = 4096; // (in bytes)

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    /** The internal ID of the stored key/value entry. The key is
        used for assigning (per hash partitioning) the entry to a
        specified machine. */
    public final UUID internalUID;

    private PartitionDescriptor partitionDescriptor;

    /** The partition directory. The mapping of partition partitionIndex to dht partition. */
    private final Map<Integer,PartitionDescriptor> partitionDirectory;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    private Key(final UUID uid) {
        this.internalUID = Preconditions.checkNotNull(uid);
        this.partitionDirectory = new TreeMap<>();
    }

    // Copy Constructor.
    private Key(final UUID uid,
                final PartitionDescriptor partitionDescriptor,
                final Map<Integer,PartitionDescriptor> partitionDirectory) {
        this.internalUID = Preconditions.checkNotNull(uid);
        this.partitionDescriptor = Preconditions.checkNotNull(partitionDescriptor);
        this.partitionDirectory = new TreeMap<>(Preconditions.checkNotNull(partitionDirectory));
    }

    public static Key newKey(final UUID uid) { return new Key(uid); }

    public static Key copyKey(final Key key, final PartitionDescriptor pd) {
        return key == null ? null : new Key(key.internalUID, pd, key.partitionDirectory);
    }

    public static Key copyKey(final Key key) {
        return key == null ? null : new Key(key.internalUID, key.partitionDescriptor, key.partitionDirectory);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void setPartitionDescriptor(final PartitionDescriptor pd) {
        this.partitionDescriptor = Preconditions.checkNotNull(pd);
    }

    public PartitionDescriptor getPartitionDescriptor() { return this.partitionDescriptor; }

    public void addPartitionDirectoryEntry(final int index, final PartitionDescriptor pd) {
        Preconditions.checkArgument(index >= 0);
        partitionDirectory.put(index, Preconditions.checkNotNull(pd));
    }

    public Map<Integer,PartitionDescriptor> getPartitionDirectory() {
        return Collections.unmodifiableMap(partitionDirectory);
    }

    public int getPartitionIndex() { return partitionDescriptor.partitionIndex; }

    public MachineDescriptor getDHTNodeFromSegmentIndex(final int segmentIndex) {
        //Preconditions.checkArgument(segmentIndex >= 0);
        for (final PartitionDescriptor pd : partitionDirectory.values())
            if (segmentIndex >= pd.segmentBaseIndex && segmentIndex <  pd.segmentBaseIndex + pd.numberOfSegments)
                return pd.machine;

        for (final PartitionDescriptor pd : partitionDirectory.values())
            System.out.println(">>>>>> " + pd.segmentBaseIndex + " <= " + segmentIndex + " < " + (pd.segmentBaseIndex + pd.numberOfSegments));
        throw new IllegalStateException();
    }

    // ---------------------------------------------------

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (!(obj instanceof Key)) return false;
        return this.internalUID.equals(((Key)obj).internalUID);
    }

    @Override
    public int hashCode() { return internalUID.hashCode(); }

    @Override
    public int compareTo(final Key k) { return internalUID.compareTo(k.internalUID); }
}
