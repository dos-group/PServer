package de.tuberlin.pserver.runtime.dht.types.experimental;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.matrix.Matrix;

import java.io.Serializable;

public class DistributedMatrix {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static enum Partitioning {

        ROW_PARTITIONING,

        COLUMN_PARTITIONING,

        BLOCK_PARTITIONING;
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static final class MatrixDescriptor implements Serializable {

        public final long baseRowOffset;

        public final long baseColOffset;

        public final int partitionRows;

        public final int partitionCols;

        public final Partitioning partitioning;

        public final int numHorizontalBlocks;

        public final int numVerticalBlocks;

        public MatrixDescriptor(final long baseRowOffset,
                                final long baseColOffset,
                                final int partitionRows,
                                final int partitionCols,
                                final Partitioning partitioning,
                                final int numHorizontalBlocks,
                                final int numVerticalBlocks) {

            this.baseRowOffset = baseRowOffset;
            this.baseColOffset = baseColOffset;
            this.partitionRows = partitionRows;
            this.partitionCols = partitionCols;
            this.partitioning  = Preconditions.checkNotNull(partitioning);
            this.numHorizontalBlocks = numHorizontalBlocks;
            this.numVerticalBlocks = numVerticalBlocks;
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final long rows;

    private final long cols;

    private final Partitioning partitioning;

    private final Layout layout;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedMatrix(final long rows, final long cols,
                             final Partitioning partitioning,
                             final Layout layout) {

        Preconditions.checkArgument(rows >= 0);
        Preconditions.checkArgument(cols >= 0);

        this.rows = rows;
        this.cols = cols;
        this.partitioning = partitioning;
        this.layout = layout;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public MatrixDescriptor[] computePartitions(final int h, final int v) {

        Partitioning partitioning = Partitioning.BLOCK_PARTITIONING;
        if (h > 0 && v == 0) partitioning = Partitioning.ROW_PARTITIONING;
        if (h == 0 && v > 0) partitioning = Partitioning.COLUMN_PARTITIONING;

        switch (partitioning) {

            case ROW_PARTITIONING:  {
                final MatrixDescriptor[] matrixDescriptors = new MatrixDescriptor[h];
                int partitionRows = (int)(rows / h);
                int partitionCols = (int)cols;
                if (partitionRows * cols > Integer.MAX_VALUE)
                    throw new IllegalStateException();
                for (int i = 0; i < h; ++i)
                    matrixDescriptors[i] = new MatrixDescriptor(partitionRows * i, 0,
                            partitionRows, partitionCols, Partitioning.ROW_PARTITIONING, i, 0);
                return matrixDescriptors;
            }

            case COLUMN_PARTITIONING: {
                final MatrixDescriptor[] matrixDescriptors = new MatrixDescriptor[v];
                int partitionRows = (int)rows;
                int partitionCols = (int)(cols / v);
                if (partitionCols * rows > Integer.MAX_VALUE)
                    throw new IllegalStateException();
                for (int i = 0; i < v; ++i)
                    matrixDescriptors[i] = new MatrixDescriptor(0, partitionCols * i,
                            partitionRows, partitionCols, Partitioning.COLUMN_PARTITIONING, 0, i);
                return matrixDescriptors;
            }

            case BLOCK_PARTITIONING: {
                final MatrixDescriptor[] matrixDescriptors = new MatrixDescriptor[h * v];
                int partitionRows = (int)(rows / h);
                int partitionCols = (int)(cols / v);
                int k = 0;
                for (int i = 0; i < h; ++i) {
                    for (int j = 0; j < v; ++j) {
                        matrixDescriptors[k++] = new MatrixDescriptor(partitionRows * i, partitionCols * j,
                                partitionRows, partitionCols, Partitioning.BLOCK_PARTITIONING, i, j);
                    }
                }
                return matrixDescriptors;
            }

            default:
                throw new IllegalStateException();
        }
    }

    public long getGlobalOffset(final long row,
                                final long col,
                                final int h,
                                final int v) {

        switch (partitioning) {
            case ROW_PARTITIONING:
                return (row * cols + col);
            case COLUMN_PARTITIONING:
                return (col * rows + row);
            case BLOCK_PARTITIONING: {
                switch (layout) {
                    case ROW_LAYOUT:
                        return (row * (cols / v) + col) % (rows / h);
                    case COLUMN_LAYOUT:
                        return (col * (rows / h) + row) % (cols / v);
                }
            }
            default:
                throw new IllegalStateException();
        }
    }

    /*public long getGlobalOffset(final long row,
                                final long col,
                                final MatrixDescriptor descriptor) {
        switch (partitioning) {
            case ROW_PARTITIONED:    return (row * cols + col);
            case COLUMN_PARTITIONED: return (col * rows + row);
            case BLOCK_PARTITIONED:  {
                final int row_length = descriptor.partitionRows;
                final int col_length = descriptor.partitionCols;
                final int row_blocks = descriptor.numHorizontalBlocks;
                final int col_blocks = descriptor.numVerticalBlocks;
                switch (layout) {
                    case ROW_LAYOUT: {
                        final long block_base = (col / col_length) * (row_length * col_length) +
                                ((row / row_length) * (row_length * col_length) * col_blocks);
                        return (col % col_length) + ((row % row_length) * col_length) + block_base;
                    }
                    case COLUMN_LAYOUT: {
                        final long block_base = (row / row_length) * (col_length * row_length) +
                                ((col / col_length) * (col_length * row_length) * row_blocks);
                        return (row % row_length) + ((col % col_length) * row_length) + block_base;
                    }
                }
            }
            default: throw new IllegalStateException();
        }
    }*/

    // ---------------------------------------------------
    // Testing.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final DistributedMatrix dm = new DistributedMatrix(10000, 10000, Partitioning.BLOCK_PARTITIONING, Layout.ROW_LAYOUT);

        final MatrixDescriptor[] descriptors = dm.computePartitions(2, 2);

        for (final MatrixDescriptor md : descriptors) {
            System.out.println("(" + md.baseRowOffset + "," + md.baseColOffset + ")");
        }

        System.out.println(dm.getGlobalOffset(9999, 9999, 2, 2));
    }
}