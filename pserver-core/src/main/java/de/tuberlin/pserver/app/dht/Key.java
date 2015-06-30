package de.tuberlin.pserver.app.dht;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.dht.valuetypes.ByteBufferValue;
import de.tuberlin.pserver.core.infra.MachineDescriptor;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class Key implements Serializable, Comparable<Key> {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public enum DistributionMode {

        LOCAL,

        DISTRIBUTED
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static final class PartitionDescriptor implements Serializable {

        public final int partitionIndex;

        public final int partitionSize;

        public final long globalOffset;

        public final int segmentBaseIndex;

        public final int numberOfSegments;

        public final int segmentSize;

        public final MachineDescriptor machine;

        public PartitionDescriptor(final int partitionIndex,
                                   final int partitionSize,
                                   final long globalOffset,
                                   final int segmentBaseIndex,
                                   final int numberOfSegments,
                                   final int segmentSize,
                                   final MachineDescriptor machine) {

            Preconditions.checkArgument(partitionIndex >= 0);
            Preconditions.checkArgument(partitionSize > 0);
            Preconditions.checkArgument(globalOffset >= 0);
            Preconditions.checkArgument(segmentBaseIndex >= 0);
            Preconditions.checkArgument(segmentSize >= 0 && segmentSize % ByteBufferValue.DEFAULT_ALIGNMENT_SIZE == 0);

            this.partitionIndex     = partitionIndex;
            this.partitionSize      = partitionSize;
            this.globalOffset       = globalOffset;
            this.segmentBaseIndex   = segmentBaseIndex;
            this.numberOfSegments   = numberOfSegments;
            this.segmentSize        = segmentSize;
            this.machine            = Preconditions.checkNotNull(machine);
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final long serialVersionUID = -1;

    public final DistributionMode distributionMode;

    public final UUID internalUID;

    public final String name;

    private final Map<Integer,PartitionDescriptor> partitionDirectory;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    // Copy Constructor.
    protected Key(final UUID uid, final String name, final DistributionMode distributionMode) { this(uid, name, null, distributionMode); }
    protected Key(final UUID uid,
                  final String name,
                  final Map<Integer,PartitionDescriptor> partitionDirectory,
                  final DistributionMode distributionMode) {
        this.internalUID            = Preconditions.checkNotNull(uid);
        this.name                   = Preconditions.checkNotNull(name);
        this.partitionDirectory     = partitionDirectory != null
                ? new TreeMap<>(Preconditions.checkNotNull(partitionDirectory))
                : new TreeMap<>();
        this.distributionMode       = Preconditions.checkNotNull(distributionMode);
    }

    public static Key newKey(final UUID uid, final String name) { return newKey(uid, name, DistributionMode.DISTRIBUTED); }
    public static Key newKey(final UUID uid, final String name, final DistributionMode distributionMode) { return new Key(uid, name, distributionMode); }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public PartitionDescriptor getPartitionDescriptor(final int instanceID) {
        return partitionDirectory.get(instanceID);
    }

    public void addPartitionDirectoryEntry(final int index, final PartitionDescriptor pd) {
        Preconditions.checkArgument(index >= 0);
        partitionDirectory.put(index, Preconditions.checkNotNull(pd));
    }

    public Map<Integer,PartitionDescriptor> getPartitionDirectory() {
        return Collections.unmodifiableMap(partitionDirectory);
    }

    public MachineDescriptor getDHTNodeFromSegmentIndex(final int segmentIndex) {
        for (final PartitionDescriptor pd : partitionDirectory.values())
            if (segmentIndex >= pd.segmentBaseIndex && segmentIndex <  pd.segmentBaseIndex + pd.numberOfSegments)
                return pd.machine;
        throw new IllegalStateException();
    }

    public int getSegmentIDFromByteOffset(final long globalOffset) {
        return (int)globalOffset / ByteBufferValue.DEFAULT_SEGMENT_SIZE;
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

    @Override
    public String toString() { return name + ":" + internalUID.toString(); }
}
