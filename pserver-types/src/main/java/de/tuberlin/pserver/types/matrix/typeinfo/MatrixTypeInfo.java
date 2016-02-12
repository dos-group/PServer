package de.tuberlin.pserver.types.matrix.typeinfo;

import de.tuberlin.pserver.types.matrix.implementation.partitioner.AbstractMatrixPartitioner;
import de.tuberlin.pserver.types.typeinfo.DistributedTypeInfo;

public interface MatrixTypeInfo extends DistributedTypeInfo {

    long rows();

    long cols();

    long globalRows();

    long globalCols();

    long globalSizeOf();

    AbstractMatrixPartitioner partitioner();
}
