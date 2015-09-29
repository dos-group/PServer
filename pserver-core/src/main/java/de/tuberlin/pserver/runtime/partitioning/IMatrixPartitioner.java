package de.tuberlin.pserver.runtime.partitioning;

import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.SlotContext;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MatrixEntry;

public abstract class IMatrixPartitioner {

    protected final long        rows;
    protected final long        cols;
    protected final SlotContext context;

    public IMatrixPartitioner(long rows, long cols, SlotContext context) {
        this.rows    = rows;
        this.cols    = cols;
        this.context = context;
    }

    public static IMatrixPartitioner newInstance(Class<? extends IMatrixPartitioner> implClass, long rows, long cols, SlotContext slotContext) {
        IMatrixPartitioner result;
        try {
            result = implClass.getDeclaredConstructor(Long.class, Long.class, SlotContext.class).newInstance(rows, cols, slotContext);
        }
        catch(Exception e) {
            throw new RuntimeException("Failed to instantiate IMatrixPartitioner", e);
        }
        return result;
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

    public abstract int getNumRowPartitions();

    public abstract int getNumColPartitions();

}
