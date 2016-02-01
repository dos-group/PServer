package de.tuberlin.pserver.examples.experiments.regression;


import de.tuberlin.pserver.math.matrix.sparse.SparseMatrix32F;
import gnu.trove.list.TFloatList;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntFloatMap;
import gnu.trove.map.hash.TIntFloatHashMap;

public final class CSRMatrix32F {

    // ---------------------------------------------------
    // Functional Interfaces.
    // ---------------------------------------------------

    public interface RowProcessor {

        void process(int row, float[] valueList, int rowStart, int rowEnd, int[] colList);
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private long rows;
    private long cols;

    private int[] colArr;
    private int[] rowPtrArr;
    private float[] valueArr;

    private TIntList colList;
    private TIntList rowPtrList;
    private TFloatList valueList;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public CSRMatrix32F(long cols) {
        this.cols = cols;
        colList = new TIntArrayList();
        rowPtrList = new TIntArrayList();
        valueList = new TFloatArrayList();
        rowPtrList.add(colList.size());
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public long rows() { return rows; }
    public long cols() { return cols; }

    public void addRow(TIntFloatMap vector) {
        vector.forEachEntry((k, v) -> {
            colList.add(k);
            valueList.add(v);
            return true;
        });
        rowPtrList.add(colList.size());
        ++rows;
    }

    public void build() {
        colArr = colList.toArray();
        colList = null;
        rowPtrArr = rowPtrList.toArray();
        rows = rowPtrList.size();
        rowPtrList = null;
        valueArr = valueList.toArray();
        valueList = null;
    }

    public void processRows(RowProcessor processor) {
        for (int i = 0; i < rows - 1; ++i) {
            processor.process(i, valueArr, rowPtrArr[i], rowPtrArr[i + 1] - 1, colArr);
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private static int binarySearch(long[] a, int fromIndex, int toIndex, long key) {
        int low = fromIndex;
        int high = toIndex - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = a[mid];
            if (midVal < key)
                low = mid + 1;
            else if (midVal > key)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found.
    }

    int[] rowPtr = new int[2];
    int s = 0;

    private int[] getRowPtr(long[] keys, long row, long cols) {
        final long lastRowIndex = row * cols + cols - 1;
        int o = s;
        final int range = (int)(o + cols > keys.length
                ? keys.length : o + cols);
        s = binarySearch(keys, o, range, lastRowIndex);
        if (s < 0)
            s = (s * -1) - 1;
        rowPtr[0] = o;
        rowPtr[1] = s - o;
        return rowPtr;
    }

    // ---------------------------------------------------
    // Public Static Methods.
    // ---------------------------------------------------

    public static CSRMatrix32F fromSparseMatrix32F(SparseMatrix32F m) {
        CSRMatrix32F csrData = new CSRMatrix32F(m.cols());
        m.createSortedKeys();
        TIntFloatHashMap d = new TIntFloatHashMap();
        for (int i = 0; i < m.rows(); ++i) {
            int[] rowPtr = csrData.getRowPtr(m.sortedKeys, i, m.cols());
            for (int j = 0; j < rowPtr[1]; ++j) {
                long k = m.sortedKeys[rowPtr[0] + j];
                float v = m.data.get(k);
                d.put((int)(k % m.cols()), v);
            }
            csrData.addRow(d);
            d.clear();
        }
        csrData.build();
        return csrData;
    }
}