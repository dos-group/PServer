package de.tuberlin.pserver.types.matrix.metadata;


import de.tuberlin.pserver.types.matrix.implementation.partitioner.AbstractMatrixPartitioner;
import de.tuberlin.pserver.types.metadata.AbstractDistributedType;
import de.tuberlin.pserver.types.metadata.DistributionScheme;

public abstract class AbstractDistributedMatrixType extends AbstractDistributedType implements DistributedMatrixType {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final long rows;

    public final long cols;

    public final long globalRows;

    public final long globalCols;

    public final AbstractMatrixPartitioner partitioner;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    //public AbstractDistributedMatrixType(AbstractDistributedMatrixType m) {
    //    this(m.nodeID, m.nodes, m.distributionScheme(), m.partitioner.getPartitionType(), m.globalRows, m.globalCols);
    //}

    public AbstractDistributedMatrixType(int nodeID, int[] nodes, DistributionScheme distributionScheme, long globalRows, long globalCols) {
        super(nodeID, nodes, distributionScheme);
        this.globalRows     = globalRows;
        this.globalCols     = globalCols;
        this.partitioner    = AbstractMatrixPartitioner.createPartitioner(distributionScheme, this);
        this.rows = partitioner != null ? partitioner.matrixPartitionShape().rows : globalRows;
        this.cols = partitioner != null ? partitioner.matrixPartitionShape().cols : globalCols;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public long rows() { return rows; }

    public long cols() { return cols; }

    public long globalRows() { return globalRows; }

    public long globalCols() { return globalCols; }

    // ---------------------------------------------------

    public AbstractMatrixPartitioner partitioner() { return partitioner; }
}
