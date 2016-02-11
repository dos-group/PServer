package de.tuberlin.pserver.types.matrix.metadata;


import de.tuberlin.pserver.types.matrix.implementation.partitioner.MatrixPartitionShape;
import de.tuberlin.pserver.types.matrix.implementation.partitioner.MatrixPartitioner;
import de.tuberlin.pserver.types.matrix.implementation.partitioner.PartitionType;
import de.tuberlin.pserver.types.metadata.AbstractDistributedType;

public abstract class AbstractDistributedMatrixType extends AbstractDistributedType implements DistributedMatrixType {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final long globalRows;

    public final long globalCols;

    public final MatrixPartitioner partitioner;

    public final MatrixPartitionShape shape;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public AbstractDistributedMatrixType(AbstractDistributedMatrixType m) {
        this(m.nodeID, m.nodes, m.partitioner.getPartitionType(), m.globalRows, m.globalCols);
    }

    public AbstractDistributedMatrixType(int nodeID, int[] nodes, PartitionType partitionType, long globalRows, long globalCols) {
        super(nodeID, nodes);
        this.globalRows     = globalRows;
        this.globalCols     = globalCols;
        this.partitioner    = MatrixPartitioner.createPartitioner(partitionType, nodeID, nodes, globalRows, globalCols);
        this.shape          = partitioner.getPartitionShape();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public long rows() { return shape.rows; }

    public long cols() { return shape.cols; }

    public long globalRows() { return globalRows; }

    public long globalCols() { return globalCols; }

    // ---------------------------------------------------

    public MatrixPartitioner partitioner() { return partitioner; }

    public MatrixPartitionShape shape() { return shape; }
}
