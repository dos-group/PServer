package de.tuberlin.pserver.math;

import de.tuberlin.pserver.math.generators.BufferGenerator;
import de.tuberlin.pserver.math.generators.MatrixGenerator;
import de.tuberlin.pserver.math.generators.VectorGenerator;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.vector.Vector;
import org.junit.Test;

public class DMatrixTests {

    @Test
    public void testMemoryLayoutGetAndSet() {
        int rows = 10;
        int cols = 15;
        Matrix matRowLayout = MatrixGenerator.RandomDMatrix(rows, cols, Layout.ROW_LAYOUT);
        double[] dataRowLayout = matRowLayout.toArray();
        Matrix matColumnLayout = MatrixGenerator.RandomDMatrix(rows, cols, Layout.COLUMN_LAYOUT);
        double[] dataColumnLayout = matColumnLayout.toArray();
        for(int i = 0; i < rows; i++) {
            for(int j = 0; j < cols; j++) {
                // rows layout: [ < #cols elements ... > < #cols elements ... > ... ]
                //               ^   first rows  ^       ^  second rows  ^
                //               ^ rows * cols + cols
                assert(matRowLayout.get(i, j) == dataRowLayout[i * cols + j]);
                // cols layout: [ < #rows elements ... > < #rows elements ... > ... ]
                //               ^   first cols  ^       ^  second cols  ^
                //               ^ cols * rows + rows
                assert(matColumnLayout.get(i, j) == dataColumnLayout[j * rows + i]);
            }
        }
        double[] newRowData = BufferGenerator.RandomValues(rows, cols);
        double[] newColData = BufferGenerator.RandomValues(rows, cols);
        for(int i = 0; i < rows; i++) {
            for(int j = 0; j < cols; j++) {
                matRowLayout.set(i, j, newRowData[i * cols + j]);
                assert(matRowLayout.get(i, j) == newRowData[i * cols + j]);
                matColumnLayout.set(i, j, newColData[i * cols + j]);
                assert(matColumnLayout.get(i, j) == dataColumnLayout[j * rows + i]);
            }
        }
    }

    @Test
    public void testViews() {
        int rows = 10;
        int cols = 15;
        Matrix matRowLayout = MatrixGenerator.RandomDMatrix(rows, cols, Layout.ROW_LAYOUT);
        double[] dataRowLayout = matRowLayout.toArray();
        Matrix matColumnLayout = MatrixGenerator.RandomDMatrix(rows, cols, Layout.COLUMN_LAYOUT);
        double[] dataColumnLayout = matColumnLayout.toArray();
        for(int i = 0; i < rows; i++) {
            // naive case: get rows from rows-layout
            double[] rowData = new double[cols];
            System.arraycopy(dataRowLayout, i * cols, rowData, 0, cols);
            Vector rowVec = matRowLayout.rowAsVector(i);
            assert(rowVec.length() == cols);
            assert(java.util.Arrays.equals(rowVec.toArray(), rowData));
            // more difficult case: get rows from cols-layout
            Vector rowVecFromColLayout = matColumnLayout.rowAsVector(i);
            assert(rowVecFromColLayout.length() == cols);
            for(int j = 0; j < cols; j++) {
                assert(rowVecFromColLayout.get(j) == matColumnLayout.get(i,j));
            }
        }
        for(int i = 0; i < cols; i++) {
            // naive case: get cols from cols-layout
            double[] colData = new double[rows];
            System.arraycopy(dataColumnLayout, i * rows, colData, 0, rows);
            Vector colVec = matColumnLayout.colAsVector(i);
            assert(colVec.length() == rows);
            assert(java.util.Arrays.equals(colVec.toArray(), colData));
            // more difficult case: get rows from cols-layout
            Vector colVecFromRowLayout = matRowLayout.colAsVector(i);
            assert(colVecFromRowLayout.length() == rows);
            for(int j = 0; j < rows; j++) {
                assert(colVecFromRowLayout.get(j) == matRowLayout.get(j,i));
            }
        }
    }

    @Test
    public void testAssigns() {
        int rows = 10;
        int cols = 15;
        Matrix matRowLayout = MatrixGenerator.RandomDMatrix(rows, cols, Layout.ROW_LAYOUT);
        checkRowColumnAssigns(rows, cols, matRowLayout);
        checkUniAndMatrixAssign(rows, cols, matRowLayout);
        Matrix matColumnLayout = MatrixGenerator.RandomDMatrix(rows, cols, Layout.COLUMN_LAYOUT);
        checkRowColumnAssigns(rows, cols, matColumnLayout);
        checkUniAndMatrixAssign(rows, cols, matColumnLayout);
    }

    public void checkRowColumnAssigns(int rows, int cols, Matrix mat) {
        double[][] rowsRowLayout = new double[rows][cols];
        for(int i = 0; i < rows; i++) {
            rowsRowLayout[i] = mat.rowAsVector(i).toArray();
        }
        double[][] colsRowLayout = new double[cols][rows];
        for(int i = 0; i < cols; i++) {
            colsRowLayout[i] = mat.colAsVector(i).toArray();
        }
        for(int row = 0; row < rows; row++) {
            Vector vec = VectorGenerator.RandomDVector(cols);
            mat.assignRow(row, vec);
            rowsRowLayout[row] = vec.toArray();
            for(int i = 0; i < cols; i++) {
                colsRowLayout[i][row] = vec.toArray()[i];
                assert(java.util.Arrays.equals(colsRowLayout[i], mat.colAsVector(i).toArray()));
            }
            for(int i = 0; i < rows; i++) {
                assert(java.util.Arrays.equals(rowsRowLayout[i], mat.rowAsVector(i).toArray()));
            }
        }
        for(int col = 0; col < cols; col++) {
            Vector vec = VectorGenerator.RandomDVector(rows);
            mat.assignColumn(col, vec);
            colsRowLayout[col] = vec.toArray();
            for(int i = 0; i < rows; i++) {
                rowsRowLayout[i][col] = vec.toArray()[i];
                assert(java.util.Arrays.equals(rowsRowLayout[i], mat.rowAsVector(i).toArray()));
            }
            for(int i = 0; i < cols; i++) {
                assert(java.util.Arrays.equals(colsRowLayout[i], mat.colAsVector(i).toArray()));
            }
        }
    }

    public void checkUniAndMatrixAssign(int rows, int cols, Matrix mat) {
        double[] uniVals = BufferGenerator.RandomUniValues(rows, cols);
        assert(java.util.Arrays.equals(mat.assign(uniVals[0]).toArray(), uniVals));
        Matrix randMat = MatrixGenerator.RandomDMatrix(10, 15);
        assert(java.util.Arrays.equals(mat.assign(randMat).toArray(), randMat.toArray()));
    }
}
