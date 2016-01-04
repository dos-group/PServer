package de.tuberlin.pserver.new_core_runtime.io.infrastructure;

import io.netty.buffer.PooledByteBufAllocator;


/**
 * The PooledByteBufAllocator pools ByteBuf instances to improve performance and minimize memory
 * fragmentation. This implementation uses an efficient approach to memory allocation known as
 * jemalloc4 that has been adopted by a number of modern OSes. The latter implementation does not
 * pool ByteBuf instances and returns a new instance every time it’s called.
 *
 * In general, the last party to access an object is responsible for releasing it.
 */
public final class NetBufferAllocator {

    public static PooledByteBufAllocator create() { return create(8192, 11); } // TODO: TUNING HAS BIG IMPACT ON ENCODING TIME !!!
    public static PooledByteBufAllocator create(int pageSize, int maxOrder) {

        // The configured chunk size whose default is 16 MiB. Override this value by specifying two
        // parameters: pageSize and maxOrder. The default values of them are 8192 and 11 respectively.
        // Chunk size is pageSize << maxOrder. You can of course adjust these parameters by either
        // specifying system properties to override the default or by specifying constructor parameters.

        final Runtime runtime = Runtime.getRuntime();

        assert pageSize > 4096 && (pageSize & pageSize - 1) == 0;

        int chunkSize = pageSize << maxOrder;

        System.out.println("NetBufferAllocator : chunkSize = " + ((chunkSize / 1024) / 1024) + "MB");

        int minNumHeapArena = runtime.availableProcessors() * 2;

        int numHeapArena = (int) Math.min(minNumHeapArena, runtime.maxMemory() / chunkSize / 2 / 3);

        int numDirectArena = 0;

        return new PooledByteBufAllocator(numHeapArena, numDirectArena, pageSize, maxOrder);
    }
}
