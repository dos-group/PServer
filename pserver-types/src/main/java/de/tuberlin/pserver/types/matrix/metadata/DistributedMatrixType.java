package de.tuberlin.pserver.types.matrix.metadata;

import de.tuberlin.pserver.types.matrix.implementation.partitioner.MatrixPartitionShape;
import de.tuberlin.pserver.types.matrix.implementation.partitioner.MatrixPartitioner;
import de.tuberlin.pserver.types.metadata.DistributedType;

public interface DistributedMatrixType extends DistributedType {

    MatrixPartitioner partitioner();

    MatrixPartitionShape shape();

    long rows();

    long cols();

    long globalRows();

    long globalCols();

    long globalSizeOf();
}
