package de.tuberlin.pserver.types.matrix;

import de.tuberlin.pserver.types.DistributedType;
import de.tuberlin.pserver.types.matrix.partitioner.MatrixPartitioner;
import de.tuberlin.pserver.types.matrix.partitioner.MatrixPartitionShape;

public interface DistributedMatrixType extends DistributedType {

    MatrixPartitioner partitioner();

    MatrixPartitionShape shape();

    long rows();

    long cols();

    long globalRows();

    long globalCols();

    long globalSizeOf();
}
