package de.tuberlin.pserver.types.matrix.metadata;

import de.tuberlin.pserver.types.matrix.implementation.partitioner.AbstractMatrixPartitioner;
import de.tuberlin.pserver.types.metadata.DistributedType;
import de.tuberlin.pserver.types.metadata.DistributionScheme;

public interface DistributedMatrixType extends DistributedType {

    long rows();

    long cols();

    long globalRows();

    long globalCols();

    long globalSizeOf();

    AbstractMatrixPartitioner partitioner();
}
