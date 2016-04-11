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

import java.util.*;
import java.util.concurrent.*;

public final class CSRMatrix32F extends Matrix32FEmptyImpl {

    // ---------------------------------------------------
    // Functional Interfaces.
    // ---------------------------------------------------

    public interface RowProcessor {

        void process(int coreID, int row, float[] valueList, int rowStart, int rowEnd, int[] colList) throws Exception;
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

    public CSRMatrix32F() {}

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

    @Override public float get(final long row, final long col) { throw new UnsupportedOperationException(); }

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
            throw new IllegalStateException("rowPtrList.size() = " + rowPtrList.size() + " | rows() = " + rows());
        rowPtrList = null;
        valueArr = valueList.toArray();
        valueList = null;
    }

    // ---------------------------------------------------
    // Parallel Processor.
    // ---------------------------------------------------

    private final ExecutorService executorService = (ThreadPoolExecutor)
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private final class PartitionProcessor implements Runnable {

        private final int id;
        private final int startRow;
        private final int endRow;
        private final RowProcessor rowProcessor;

        public PartitionProcessor(int id, int startRow, int endRow, RowProcessor rowProcessor) {
            this.id           = id;
            this.startRow     = startRow;
            this.endRow       = endRow;
            this.rowProcessor = rowProcessor;
        }

        @Override
        public void run() {
            try {
                for (int i = startRow; i < endRow; ++i) {
                    rowProcessor.process(id, i, valueArr, rowPtrArr[i], rowPtrArr[i + 1], colArr);
                }
            } catch(Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public void processRows(RowProcessor processor) {
        processRows(Runtime.getRuntime().availableProcessors(), processor);
    }

    public void processRows(int dop, RowProcessor processor) {
        Collection<Future<?>> futures = new LinkedList<>();
        int partitionSize = (int)rows() / dop;
        int lastPartitionRest = (int)rows() % dop;
        for (int id = 0; id < dop; ++id) {
            int startRow = id * partitionSize;
            int endRow = id * partitionSize + partitionSize + ((id == dop - 1) ? lastPartitionRest : 0);
            futures.add(executorService.submit(new PartitionProcessor(id, startRow, endRow, processor)));
        }
        try {
            for (Future<?> future:futures)
                future.get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

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