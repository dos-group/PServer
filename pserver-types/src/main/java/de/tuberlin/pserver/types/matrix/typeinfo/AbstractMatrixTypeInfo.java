package de.tuberlin.pserver.types.matrix.typeinfo;


import de.tuberlin.pserver.types.matrix.implementation.partitioner.AbstractMatrixPartitioner;
import de.tuberlin.pserver.types.typeinfo.AbstractDistributedTypeInfo;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;

public abstract class AbstractMatrixTypeInfo extends AbstractDistributedTypeInfo implements MatrixTypeInfo {

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
    //    this(m.nodeID, m.nodes, m.distScheme(), m.partitioner.getPartitionType(), m.globalRows, m.globalCols);
    //}

    public AbstractMatrixTypeInfo() {
        this.rows = -1;
        this.cols = -1;
        this.globalRows     = -1;
        this.globalCols     = -1;
        this.partitioner    = null;
    }

    public AbstractMatrixTypeInfo(int nodeID, int[] nodes, Class<?> type, String name, DistScheme distScheme, long globalRows, long globalCols) {
        super(nodeID, nodes, type, name, distScheme);
        this.globalRows     = globalRows;
        this.globalCols     = globalCols;
        this.partitioner    = AbstractMatrixPartitioner.createPartitioner(distScheme, this);
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
