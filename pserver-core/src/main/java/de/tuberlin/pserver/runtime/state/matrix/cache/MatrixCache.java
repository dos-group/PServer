package de.tuberlin.pserver.runtime.state.matrix.cache;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.RuntimeContext;
import de.tuberlin.pserver.runtime.state.matrix.partitioner.MatrixPartitioner;

import java.util.concurrent.CountDownLatch;

public final class MatrixCache<V extends Number, T extends Matrix<V>> {

    // ---------------------------------------------------
    // Cache EntryImpl.
    // ---------------------------------------------------

    public static final class CacheEntry {

    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final RuntimeContext runtimeContext;

    private final int[] cacheProviderNodes;

    private final MatrixPartitioner partitioner;

    private final Matrix<V> matrix;

    private final LRUCache<Long, CacheEntry> lruCache;

    private CountDownLatch pullRequestLatch;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MatrixCache(final RuntimeContext runtimeContext,
                       final int[] cacheProviderNodes,
                       final MatrixPartitioner partitioner,
                       final Matrix<V> matrix,
                       final int cacheSize) {

        this.runtimeContext = Preconditions.checkNotNull(runtimeContext);

        this.cacheProviderNodes = Preconditions.checkNotNull(cacheProviderNodes);

        this.partitioner = Preconditions.checkNotNull(partitioner);

        this.matrix = Preconditions.checkNotNull(matrix);

        this.lruCache = new LRUCache<>(cacheSize);

        this.pullRequestLatch = new CountDownLatch(1);

        registerHandlers();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public V getValue(final long row, final long col) throws Exception {

        final long index = row * matrix.cols() + col;

        if (!lruCache.containsKey(index)) {

            runtimeContext.netManager.dispatchEventAt(
                    cacheProviderNodes,
                    new CacheEvent(
                            CacheEvent.CACHE_PULL_REQUEST_EVENT,
                            new long[] { index },
                            null
                    )
            );

            pullRequestLatch.await();
        }

        return matrix.get(index);
    }

    public void putValue(final long row, final long col, final V value) {

        final long index = row * matrix.cols() + col;

        matrix.set(row, col, value);

        lruCache.putIfAbsent(index, new CacheEntry());
    }

    // ---------------------------------------------------

    public void writeDirtyCacheEntries() {

        final long[] elementIndices = new long[lruCache.getDirtyEntryKeys().size()];

        final long[] elementValues = new long[lruCache.getDirtyEntryKeys().size()];

        for (int i = 0; i < lruCache.getDirtyEntryKeys().size(); ++i) {

            elementIndices[i] = lruCache.getDirtyEntryKeys().get(i);

            elementValues[i] = matrix.toLong(matrix.get(elementIndices[i]));
        }

        push(elementIndices, elementValues);

        lruCache.clearDirtyEntries();
    }

    // ---------------------------------------------------

    public void pull(final long[] elementIndices) {

        runtimeContext.netManager.dispatchEventAt(
                cacheProviderNodes,
                new CacheEvent(
                        CacheEvent.CACHE_PULL_REQUEST_EVENT,
                        elementIndices,
                        null
                )
        );
    }

    public void push(final long[] elementIndices, final long[] elementValues) {

        Preconditions.checkNotNull(elementIndices.length == elementValues.length);

        if (partitioner == null) {

            runtimeContext.netManager.dispatchEventAt(
                    cacheProviderNodes,
                    new CacheEvent(
                            CacheEvent.CACHE_PULL_RESPONSE_EVENT,
                            elementIndices,
                            elementValues
                    )
            );
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void registerHandlers() {

        runtimeContext.netManager.addEventListener(CacheEvent.CACHE_PULL_RESPONSE_EVENT, e -> {

            final CacheEvent event = (CacheEvent)e;

            for (int i = 0; i < event.elementIndices.length; ++i) {

                matrix.set(event.elementIndices[i] / matrix.cols(), event.elementIndices[i] % matrix.cols(), matrix.fromLong(event.elementValues[i]));

                lruCache.putIfAbsent(event.elementIndices[i], new CacheEntry());
            }

            pullRequestLatch.countDown();
        });

        runtimeContext.netManager.addEventListener(CacheEvent.CACHE_PUSH_RESPONSE_EVENT, e -> {

            throw new IllegalStateException();
        });
    }
}
