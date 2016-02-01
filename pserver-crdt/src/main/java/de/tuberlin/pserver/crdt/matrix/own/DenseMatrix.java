package de.tuberlin.pserver.crdt.matrix.own;

import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.crdt.operations.TaggedOperation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import org.apache.commons.lang3.ArrayUtils;

// TODO: implement!

/*
 * Array layout is critical for correctly passing arrays between programs written in different languages. It is also
 * important for performance when traversing an array because accessing array elements that are contiguous in memory is
 * usually faster than accessing elements which are not, due to caching.[1] In some media such as tape or NAND flash memory,
 * accessing sequentially is orders of magnitude faster than nonsequential access.
 */
// https://en.wikipedia.org/wiki/Row-major_order

// Using row-major order
public class DenseMatrix<T> extends AbstractMatrix<T> {

    private float[] data;

    /**
     * Sole constructor
     *
     * @param id             the ID of the CRDT that this replica belongs to
     * @param noOfReplicas
     * @param programContext the {@code RuntimeManager} belonging to this {@code MLProgram}
     */
    public DenseMatrix(long rows, long cols, String id, int noOfReplicas, ProgramContext programContext) {
        super(rows, cols, id, noOfReplicas, programContext);
        this.data = new float[toInt(rows * cols)];
        //Arrays.fill(this.data, 0);
    }


    @Override
    protected boolean update(int srcNodeId, Operation<?> op) {
        TaggedOperation<Float, Coordinates> mop = (TaggedOperation<Float, Coordinates>) op;

        if (mop.getType() == Operation.OpType.SET) {
            return setValue(mop.getTag().getRow(), mop.getTag().getCol(), mop.getValue());
        } else {
            throw new IllegalArgumentException("GCounter CRDTs do not allow the " + op.getType() + " operation.");
        }
    }

    public Float get(final long row, final long col) {
        return data[getPosition(row, col)];
    }

    public float[] getRow(long i) {
        // TODO: bounds checks
        return ArrayUtils.subarray(data, (int)(cols*i), (int)((cols*i) + cols) );
    }

    public boolean set(final long row, final long col, final float value) {
        if(setValue(row, col, value)) {
            broadcast(new TaggedOperation<>(Operation.OpType.SET, value, new Coordinates(row, col) ));
            return true;
        }
        return false;
    }

    public boolean setRow(float[] values, int row) {
        // TOdo: check bounds
        for(int i = 0; i < values.length; i++) {
            set(row, i, values[i]);
        }
        return true;
    }

    public long getRows() {
        return this.rows;
    }

    private boolean setValue(long row, long col, float value) {
        checkBounds(row, col);
        data[getPosition(row, col)] = value;
        return true;
    }

    private int toInt(long value) {
        if(value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            return (int) value;
        }
        else {
            throw new RuntimeException("Out of int range");
        }
    }

    private int getPosition(long row, long col) {
        return toInt(row * cols + col);
    }
}
