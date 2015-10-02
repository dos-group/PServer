package de.tuberlin.pserver.runtime.partitioning;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.SlotContext;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MatrixEntry;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public abstract class IMatrixPartitioner {

    protected final long  rows;
    protected final long  cols;
    protected final int   nodeId;
    protected final int[] atNodes;

    public IMatrixPartitioner(long rows, long cols, int nodeId, int[] atNodes) {
        boolean contained = false;
        for(int node : atNodes) {
            if(node == nodeId) {
                contained = true;
                break;
            }
        }
        Preconditions.checkArgument(
                contained,
                "Failed to instantiate IMatrixPartitioner: nodeId '%s' is not contained in nodeList: %s",
                nodeId,
                Arrays.toString(atNodes)
        );
        this.rows    = rows;
        this.cols    = cols;
        this.nodeId  = nodeId;
        this.atNodes = atNodes;
    }

    public IMatrixPartitioner(long rows, long cols, int nodeId, int numNodes) {
        this(rows, cols, nodeId, IntStream.iterate(0, x -> x + 1).limit(numNodes).toArray());
    }

    public static IMatrixPartitioner newInstance(Class<? extends IMatrixPartitioner> implClass, long rows, long cols, int nodeId, int[] atNodes) {
        IMatrixPartitioner result;
        try {
            result = implClass.getDeclaredConstructor(long.class, long.class, int.class, int[].class).newInstance(rows, cols, nodeId, atNodes);
        }
        catch(NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException("Failed to instantiate IMatrixPartitioner implementation " + implClass.getName(), e);
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

    public int getNodeId() {
        return nodeId;
    }

    public int[] getNodes() {
        return atNodes;
    }

    public abstract IMatrixPartitioner ofNode(int nodeId);

}
