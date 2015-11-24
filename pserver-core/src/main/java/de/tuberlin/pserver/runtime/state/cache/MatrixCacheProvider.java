package de.tuberlin.pserver.runtime.state.cache;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.RuntimeContext;

public class MatrixCacheProvider<V extends Number, T extends Matrix<V>>{

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final RuntimeContext runtimeContext;

    private final Matrix<V> matrix;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MatrixCacheProvider(final RuntimeContext runtimeContext,
                               final Matrix<V> matrix) {

        this.runtimeContext = Preconditions.checkNotNull(runtimeContext);

        this.matrix = Preconditions.checkNotNull(matrix);

        registerHandlers();
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void registerHandlers() {

        runtimeContext.netManager.addEventListener(CacheEvent.CACHE_PULL_REQUEST_EVENT, e -> {

            final CacheEvent event = (CacheEvent)e;

            final long[] elementValues = new long[event.elementIndices.length];

            for (int i = 0; i < event.elementIndices.length; ++i) {

                elementValues[i] = matrix.toLong(matrix.get(event.elementIndices[i]));
            }

            runtimeContext.netManager.sendEvent(
                    event.srcMachineID,
                    new CacheEvent(CacheEvent.CACHE_PULL_RESPONSE_EVENT, null, elementValues)
            );
        });

        runtimeContext.netManager.addEventListener(CacheEvent.CACHE_PUSH_REQUEST_EVENT, e -> {

            final CacheEvent event = (CacheEvent)e;

            for (int i = 0; i < event.elementIndices.length; ++i) {

                matrix.set(event.elementIndices[i] / matrix.cols(), event.elementIndices[i] % matrix.cols(), matrix.fromLong(event.elementValues[i]));
            }
        });
    }
}
