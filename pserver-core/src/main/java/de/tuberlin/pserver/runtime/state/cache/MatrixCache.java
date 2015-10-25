package de.tuberlin.pserver.runtime.state.cache;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.RuntimeContext;
import de.tuberlin.pserver.runtime.state.partitioner.IMatrixPartitioner;

public final class MatrixCache<V extends Number, T extends Matrix<V>> {

    // ---------------------------------------------------
    // Cache Entry.
    // ---------------------------------------------------

    public static final class CacheEntry {

        // TODO
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final RuntimeContext runtimeContext;

    private final int[] cacheProviderNodes;

    private final IMatrixPartitioner partitioner;

    private final Matrix<V> matrix;

    private final LRUCache<Long, CacheEntry> lruCache;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MatrixCache(final RuntimeContext runtimeContext,
                       final int[] cacheProviderNodes,
                       final IMatrixPartitioner partitioner,
                       final Matrix<V> matrix,
                       final int cacheSize) {

        this.runtimeContext = Preconditions.checkNotNull(runtimeContext);

        this.cacheProviderNodes = Preconditions.checkNotNull(cacheProviderNodes);

        this.partitioner = Preconditions.checkNotNull(partitioner);

        this.matrix = Preconditions.checkNotNull(matrix);

        this.lruCache = new LRUCache<>(cacheSize);

        registerHandlers();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public V getValue(final long row, final long col) {

        final long index = row * matrix.cols() + col;

        if (lruCache.containsKey(index))
            return matrix.get(index);
        else
            throw new IllegalStateException();
    }

    public void putValue(final long row, final long col, final V value) {

        final long index = row * matrix.cols() + col;

        matrix.set(row, col, value);

        lruCache.putIfAbsent(index, new CacheEntry());
    }

    // ---------------------------------------------------

    public void pull(final long[] elementIndices) {

        runtimeContext.netManager.sendEvent(
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

            runtimeContext.netManager.sendEvent(
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
        });

        runtimeContext.netManager.addEventListener(CacheEvent.CACHE_PUSH_RESPONSE_EVENT, e -> {

            throw new IllegalStateException();
        });
    }
}
