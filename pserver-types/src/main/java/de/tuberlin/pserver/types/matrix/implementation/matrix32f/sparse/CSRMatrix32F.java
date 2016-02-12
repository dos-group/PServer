package de.tuberlin.pserver.types.matrix.implementation.matrix32f.sparse;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;
import de.tuberlin.pserver.types.typeinfo.properties.InternalData;
import gnu.trove.list.TFloatList;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntFloatMap;

import java.util.Random;

public final class CSRMatrix32F extends Matrix32FEmptyImpl {

    // ---------------------------------------------------
    // Functional Interfaces.
    // ---------------------------------------------------

    public interface RowProcessor {

        void process(int row, float[] valueList, int rowStart, int rowEnd, int[] colList);
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private int[] colArr;
    private int[] rowPtrArr;
    private float[] valueArr;

    transient private TIntList colList;
    transient private TIntList rowPtrList;
    transient private TFloatList valueList;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public CSRMatrix32F() {
    }

    // Local Constructor.
    public CSRMatrix32F(long globalRows, long globalCols) {
        this(-1, null, null, null, DistScheme.LOCAL, globalRows, globalCols);
    }

    // Global Constructor.
    public CSRMatrix32F(int nodeID, int[] nodes, Class<?> type, String name, DistScheme distScheme, long globalRows, long globalCols) {
        super(nodeID, nodes, type, name, distScheme, globalRows, globalCols, null);
        colList = new TIntArrayList();
        rowPtrList = new TIntArrayList();
        valueList = new TFloatArrayList();
        rowPtrList.add(colList.size());
    }

    // ---------------------------------------------------
    // Distributed Type Metadata.
    // ---------------------------------------------------

    @Override public long sizeOf() { return valueArr != null ? valueArr.length * Float.BYTES + (rowPtrArr.length + colArr.length) * Integer.BYTES : -1; }

    @Override public long globalSizeOf() { throw new UnsupportedOperationException(); }

    @SuppressWarnings("unchecked")
    @Override public InternalData<Object[]> internal() { return new InternalData<>(new Object[] {rowPtrArr, colArr, valueArr}); }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override public float get(final long row, final long col) {
        return 0f;
    }

    public void addRow(TIntFloatMap vector) {
        vector.forEachEntry((k, v) -> {
            colList.add(k);
            valueList.add(v);
            return true;
        });
        rowPtrList.add(colList.size());
    }

    public void build() {
        colArr = colList.toArray();
        colList = null;
        rowPtrArr = rowPtrList.toArray();
        if (rowPtrList.size() - 1 != rows())
            throw new IllegalStateException();
        rowPtrList = null;
        valueArr = valueList.toArray();
        valueList = null;
    }

    public void processRows(RowProcessor processor) {
        for (int i = 0; i < rows() - 1; ++i) {
            processor.process(i, valueArr, rowPtrArr[i], rowPtrArr[i + 1] - 1, colArr);
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /*private static int binarySearch(long[] a, int fromIndex, int toIndex, long key) {
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
    }*/

    // ---------------------------------------------------
    // Public Static Methods.
    // ---------------------------------------------------

    /*public static CSRMatrix32F fromSparseMatrix32F(SparseMatrix32F m) {
        CSRMatrix32F csrData = new CSRMatrix32F(m);
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
    }*/

    // ---------------------------------------------------
    // ROW ITERATOR.
    // ---------------------------------------------------

    @Override
    public RowIterator rowIterator() {
        return new RowIterator(this);
    }

    @Override
    public RowIterator rowIterator(final long startRow, final long endRow) {
        return new RowIterator(this, startRow, endRow);
    }

    // ---------------------------------------------------

    private static final class RowIterator implements Matrix32F.RowIterator {

        private CSRMatrix32F self;

        private final long end;

        private final long start;

        private long currentRow;

        private final long rowsToFetch;

        private long rowsFetched;

        private Random rand;

        // ---------------------------------------------------

        public RowIterator(final Matrix32F m) {
            this(m, 0, m.rows());
        }

        public RowIterator(final Matrix32F m, final long startRow, final long endRow) {
            this.self = (CSRMatrix32F) m;
            Preconditions.checkArgument(startRow >= 0 && startRow < self.rows());
            Preconditions.checkArgument(endRow >= startRow && endRow <= self.rows());
            this.start = startRow;
            this.end = endRow;
            this.rowsToFetch = endRow - startRow;
            this.rand = new Random();
            reset();
        }

        // ---------------------------------------------------

        @Override public boolean hasNext() { return rowsFetched < rowsToFetch; }

        @Override
        public void next() {
            // the generic case is just currentRow++, but the reset method only sets rowsFetched = 0
            if (rowsFetched == 0)
                currentRow = 0;
            else
                currentRow++;
            rowsFetched++;
            // can overflow if nextRandom and next is called alternatingly
            if (currentRow >= end)
                currentRow = start;
        }

        @Override
        public void nextRandom() {
            rowsFetched++;
            currentRow = start + rand.nextInt((int) end);
        }

        @Override
        public float value(final long col) {
            throw new UnsupportedOperationException();
        }

        @Override public Matrix32F get() { return get(0, (int) self.cols()); }

        @Override public Matrix32F get(final long from, final long size) {
            SparseMatrix32F res = new SparseMatrix32F(1, size);
            for (int i = self.rowPtrArr[(int)currentRow] + (int)from; i < self.rowPtrArr[(int)currentRow + 1] - 1; ++i)
                res.set(0, self.colArr[i], self.valueArr[i]);
            return res;
        }

        @Override public void reset() { rowsFetched = 0; }

        @Override public long size() { return rowsToFetch; }

        @Override public long rowNum() { return currentRow; }
    }
}