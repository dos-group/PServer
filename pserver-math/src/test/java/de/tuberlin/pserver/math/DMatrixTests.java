package de.tuberlin.pserver.math;

import de.tuberlin.pserver.math.generators.BufferGenerator;
import de.tuberlin.pserver.math.generators.MatrixGenerator;
import org.junit.Test;

public class DMatrixTests {

    @Test
    public void testMemoryLayoutGetAndSet() {
        int rows = 10;
        int cols = 15;
        Matrix matRowLayout = MatrixGenerator.RandomDMatrix(rows, cols, DMatrix.MemoryLayout.ROW_LAYOUT);
        double[] dataRowLayout = matRowLayout.toArray();
        Matrix matColumnLayout = MatrixGenerator.RandomDMatrix(rows, cols, DMatrix.MemoryLayout.COLUMN_LAYOUT);
        double[] dataColumnLayout = matColumnLayout.toArray();
        for(int i = 0; i < rows; i++) {
            for(int j = 0; j < cols; j++) {
                // row layout: [ < #cols elements ... > < #cols elements ... > ... ]
                //               ^   first row  ^       ^  second row  ^
                //               ^ row * numCols + col
                assert(matRowLayout.get(i, j) == dataRowLayout[i * cols + j]);
                // col layout: [ < #rows elements ... > < #rows elements ... > ... ]
                //               ^   first col  ^       ^  second col  ^
                //               ^ col * numRows + row
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
        Matrix matRowLayout = MatrixGenerator.RandomDMatrix(rows, cols, DMatrix.MemoryLayout.ROW_LAYOUT);
        double[] dataRowLayout = matRowLayout.toArray();
        Matrix matColumnLayout = MatrixGenerator.RandomDMatrix(rows, cols, DMatrix.MemoryLayout.COLUMN_LAYOUT);
        double[] dataColumnLayout = matColumnLayout.toArray();
        for(int i = 0; i < rows; i++) {
            // naive case: get rows from row-layout
            double[] rowData = new double[cols];
            System.arraycopy(dataRowLayout, i * cols, rowData, 0, cols);
            Vector rowVec = matRowLayout.viewRow(i);
            assert(rowVec.size() == cols);
            assert(java.util.Arrays.equals(rowVec.toArray(), rowData));
            // more difficult case: get rows from col-layout
            Vector rowVecFromColLayout = matColumnLayout.viewRow(i);
            assert(rowVecFromColLayout.size() == cols);
            for(int j = 0; j < cols; j++) {
                assert(rowVecFromColLayout.get(j) == matColumnLayout.get(i,j));
            }
        }
        for(int i = 0; i < cols; i++) {
            // naive case: get cols from col-layout
            double[] colData = new double[rows];
            System.arraycopy(dataColumnLayout, i * rows, colData, 0, rows);
            Vector colVec = matColumnLayout.viewColumn(i);
            assert(colVec.size() == rows);
            assert(java.util.Arrays.equals(colVec.toArray(), colData));
            // more difficult case: get rows from col-layout
            Vector colVecFromRowLayout = matRowLayout.viewColumn(i);
            assert(colVecFromRowLayout.size() == rows);
            for(int j = 0; j < rows; j++) {
                assert(colVecFromRowLayout.get(j) == matRowLayout.get(j,i));
            }
        }
    }
}
