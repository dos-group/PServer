package de.tuberlin.pserver.math.delegates.sparse.mtj;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.*;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.Vector;
import no.uib.cipr.matrix.*;
import no.uib.cipr.matrix.AbstractMatrix;
import no.uib.cipr.matrix.sparse.*;

import java.util.Iterator;

public class MTJMatrix extends SMatrix<no.uib.cipr.matrix.Matrix> {

    public MTJMatrix(long rows, long cols, no.uib.cipr.matrix.Matrix target, MemoryLayout layout) {
        super(rows, cols, target, layout);
    }

    @Override
    public double get(long row, long col) { return target.get((int)row, (int)col); }

    @Override
    public void set(long row, long col, double value) { target.set((int)row, (int)col, value); }

    @Override
    public double[] toArray() {
        double[] result = new double[(int)(rows*cols)];
        Iterator<MatrixEntry> iter = target.iterator();
        while(iter.hasNext()) {
            MatrixEntry entry = iter.next();
            result[Utils.getPos(entry.row(), entry.column(), layout, rows, cols)] = entry.get();
        }
        return result;
    }

    private int[][] buildNz(double[] data) {
        int[][] nz;
        switch(layout) {
            case COMPRESSED_ROW:
                nz = new int[Utils.toInt(rows)][];
                for(long i = 0; i < rows; i++) {
                    int[] buffer = new int[Utils.toInt(cols)];
                    int bufLength = 0;
                    for(long j = 0; j < cols; j++) {
                        if(data[Utils.getPos(i, j, layout, rows, cols)] != 0.0) {
                            buffer[bufLength] = Utils.toInt(j);
                            bufLength++;
                        }
                    }
                    nz[Utils.toInt(i)] = java.util.Arrays.copyOf(buffer, bufLength);
                }
                break;
            case COMPRESSED_COL:
                nz = new int[Utils.toInt(cols)][];
                for(long i = 0; i < cols; i++) {
                    int[] buffer = new int[Utils.toInt(rows)];
                    int bufLength = 0;
                    for(long j = 0; j < rows; j++) {
                        if(data[Utils.getPos(i, j, layout, rows, cols)] != 0.0) {
                            buffer[bufLength] = Utils.toInt(j);
                            bufLength++;
                        }
                    }
                    nz[Utils.toInt(i)] = java.util.Arrays.copyOf(buffer, bufLength);
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown MemoryLayout: " + layout.toString());
        }
        return nz;
    }

    private AbstractMatrix getNewInstance(int[][] nz) {
        AbstractMatrix result;
        switch(layout) {
            case COMPRESSED_ROW:
                result = new CompRowMatrix(Utils.toInt(rows), Utils.toInt(cols), nz);
                break;
            case COMPRESSED_COL:
                result = new CompColMatrix(Utils.toInt(rows), Utils.toInt(cols), nz);
                break;
            default:
                throw new IllegalArgumentException("Unknown MemoryLayout: " + layout.toString());
        }
        return result;
    }

    @Override
    public void setArray(double[] data) {
        Preconditions.checkArgument(data.length == rows * cols, String.format("Wrong length of data array. Excepted: rows * cols = %d * %d = %d. Actual: %d", rows, cols, rows*cols, data.length));
        target = getNewInstance(buildNz(data));
        for(long i = 0; i < rows; i++) {
            for(long j = 0; j < cols; j++) {
                double val = data[Utils.getPos(i, j, layout, rows, cols)];
                if(val != 0.0) {
                    target.set(Utils.toInt(i), Utils.toInt(j), val);
                }
            }
        }
    }

    @Override
    public Matrix assign(Matrix v) {
        return null;
    }

    @Override
    public Vector viewRow(long row) {
        return null;
    }

    @Override
    public Vector viewColumn(long col) {
        return null;
    }

    @Override
    public Matrix assignRow(long row, Vector v) {
        return null;
    }

    @Override
    public Matrix assignColumn(long col, Vector v) {
        return null;
    }
}
