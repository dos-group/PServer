package de.tuberlin.pserver.runtime.state.partitioner;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.matrix.partitioning.PartitionShape;
import de.tuberlin.pserver.runtime.state.entries.Entry;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.stream.IntStream;

public abstract class MatrixPartitioner {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final long  rows;

    protected final long  cols;

    protected final int   nodeId;

    protected final int[] atNodes;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MatrixPartitioner() { this(-1, -1, -1, null); }
    public MatrixPartitioner(long rows, long cols, int nodeId, int numNodes) {
        this(rows, cols, nodeId, IntStream.iterate(0, x -> x + 1).limit(numNodes).toArray());
    }
    public MatrixPartitioner(long rows, long cols, int nodeId, int[] atNodes) {
        /*boolean contained = false;
        for(int node : atNodes) {
            if(node == nodeId) {
                contained = true;
                break;
            }
        }
        Preconditions.checkArgument(
                contained,
                "Failed to instantiate MatrixPartitioner: nodeId '%s' is not contained in nodeList: %s",
                nodeId,
                Arrays.toString(atNodes)
        );*/
        this.rows    = rows;
        this.cols    = cols;
        this.nodeId  = nodeId;
        this.atNodes = atNodes;
    }

    // ---------------------------------------------------
    // Abstract Methods.
    // ---------------------------------------------------

    public abstract int getPartitionOfEntry(long row, long col);

    public abstract int getPartitionOfEntry(Entry entry);

    public abstract PartitionShape getPartitionShape();

    public abstract long translateGlobalToLocalRow(long row) throws IllegalArgumentException;

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

    public abstract MatrixPartitioner ofNode(int nodeId);


    public static MatrixPartitioner newInstance(Class<? extends MatrixPartitioner> implClass, long rows, long cols, int nodeId, int[] atNodes) {
        MatrixPartitioner result;
        try {
            result = implClass.getDeclaredConstructor(long.class, long.class, int.class, int[].class).newInstance(rows, cols, nodeId, atNodes);
        }
        catch(NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException("Failed to instantiate IMatrixPartitioner implementation " + implClass.getName(), e);
        }
        return result;
    }
}
