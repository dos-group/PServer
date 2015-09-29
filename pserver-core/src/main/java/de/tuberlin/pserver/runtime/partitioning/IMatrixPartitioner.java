package de.tuberlin.pserver.runtime.partitioning;

import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.SlotContext;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MatrixEntry;

public abstract class IMatrixPartitioner {

    protected final PartitioningConfig config;
    protected final SlotContext   context;

    public IMatrixPartitioner(PartitioningConfig config, SlotContext context) {
        this.config  = config;
        this.context = context;
    }

    public abstract int getPartitionOfEntry(MatrixEntry entry);

    public abstract Matrix.PartitionShape getPartitionShape();

    /**
     * Given a "global" row in the complete matrix, this method translates it's position in the current partition.
     */
    public abstract long translateGlobalToLocalRow(long row) throws IllegalArgumentException;

    /**
     * Given a "global" col in the complete matrix, this method translates it's position in the current partition.
     */
    public abstract long translateGlobalToLocalCol(long col) throws IllegalArgumentException;

    public abstract long translateLocalToGlobalRow(long row) throws IllegalArgumentException;

    public abstract long translateLocalToGlobalCol(long col) throws IllegalArgumentException;

}
