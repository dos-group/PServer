package de.tuberlin.pserver.app.disttypes;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.utils.ObjectSerializer;
import de.tuberlin.pserver.utils.UnsafeOp;
import org.apache.commons.lang3.tuple.Triple;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

public class DistributedArray<T extends Serializable> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final long numElements;

    private final Class<T> elementType;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedArray(final long numElements, final Class<T> elementType) {
        this.numElements = numElements;
        this.elementType = Preconditions.checkNotNull(elementType);
    }

    // ---------------------------------------------------

    public static final class MemoryBlock {

        public static final int MEMORY_BLOCK_SIZE = 4 * 1024; // 4K.

        public final int offset;

        public final int length;

        public final byte[] buffer;

        public MemoryBlock(final byte[] buffer, final int offset, final int length) {
            this.buffer = Preconditions.checkNotNull(buffer);
            this.offset = offset;
            this.length = length;
        }

        public static MemoryBlock allocHeapBlock() {
            return new MemoryBlock(new byte[MEMORY_BLOCK_SIZE], 0, MEMORY_BLOCK_SIZE);
        }
    }

    // ---------------------------------------------------

    public static final class SerializedArray<T> {

        private final ObjectSerializer serializer;

        private final List<MemoryBlock> blocks;

        private MemoryBlock currentBlock;

        private int areaSize;

        private final Map<Integer,Triple<MemoryBlock,Integer,Integer>> indexOffsetMap;

        private final int size;

        private int currentBlockOffset;

        // ---------------------------------------------------

        public SerializedArray(final int size) {
            this(ObjectSerializer.Factory.create(ObjectSerializer.SerializerType.JAVA_SERIALIZER), size);
        }

        public SerializedArray(final ObjectSerializer serializer, final int size) {
            this.serializer = Preconditions.checkNotNull(serializer);
            this.size = size;
            this.blocks = new ArrayList<>();
            this.indexOffsetMap = new HashMap<>();
            final MemoryBlock mb = MemoryBlock.allocHeapBlock();
            this.blocks.add(mb);
            this.currentBlock = mb;
            this.areaSize += mb.length;
        }

        // ---------------------------------------------------

        public void store(final int idx, final T obj) {
            Preconditions.checkNotNull(obj);
            Preconditions.checkArgument(idx < size);
            final byte[] data = serializer.serialize(obj);
            if (!indexOffsetMap.containsKey(idx))
                store0(true, idx, data, currentBlock, currentBlockOffset);
            else {
                final Triple<MemoryBlock,Integer,Integer> elementEntry = indexOffsetMap.get(idx);
                final MemoryBlock mb = elementEntry.getLeft();
                final int offset = elementEntry.getMiddle();
                final int size = elementEntry.getRight();
                if (data.length > size)
                    throw new IllegalStateException();
                store0(false, idx, data, mb, offset);
            }
        }

        public T fetch(final int idx) {
            final Triple<MemoryBlock,Integer,Integer> elementEntry = indexOffsetMap.get(idx);
            if (elementEntry == null)
                return null;

            final MemoryBlock mb = elementEntry.getLeft();
            final int offset = elementEntry.getMiddle();
            final int size = elementEntry.getRight();
            final byte[] data = new byte[size];

            if (mb.length - offset >= size) {
                System.arraycopy(mb.buffer, offset, data, 0, size);
            } else {
                final int firstPartLength = mb.length - offset;
                System.arraycopy(mb.buffer, offset, data, 0, firstPartLength);
                final int nextBlockIdx = blocks.indexOf(mb) + 1;
                Preconditions.checkState(nextBlockIdx < blocks.size());
                final MemoryBlock nextMb = blocks.get(nextBlockIdx);
                final int secondPartLength = size - firstPartLength;
                System.arraycopy(nextMb.buffer, 0, data, firstPartLength, secondPartLength);
            }
            return serializer.deserialize(data);
        }

        public int getSize() { return areaSize; }

        // ---------------------------------------------------

        private void store0(final boolean isInitialStore,
                            final int idx,
                            final byte[] data,
                            MemoryBlock block,
                            int blockOffset) {

            if (data.length <= block.length - blockOffset) {
                System.arraycopy(data, 0, block.buffer, blockOffset, data.length);
                indexOffsetMap.put(idx, Triple.of(block, blockOffset, data.length));
                if (isInitialStore)
                    currentBlockOffset += data.length;
            } else {
                final int firstPartLength = block.length - blockOffset;
                System.arraycopy(data, 0, block.buffer, blockOffset, firstPartLength);
                final MemoryBlock oldBlock = block;
                final int oldBlockOffset = blockOffset;
                if (isInitialStore) {
                    allocNextBlock();
                    block = currentBlock;
                } else {
                    final int nextBlockIdx = blocks.indexOf(oldBlock) + 1;
                    Preconditions.checkState(nextBlockIdx < blocks.size());
                    block = blocks.get(nextBlockIdx);
                }
                blockOffset = currentBlockOffset;
                final int secondPartLength = data.length - firstPartLength;
                System.arraycopy(data, firstPartLength, block.buffer, blockOffset, secondPartLength);
                indexOffsetMap.put(idx, Triple.of(oldBlock, oldBlockOffset, data.length));
                if (isInitialStore)
                    currentBlockOffset += secondPartLength;
            }
        }

        private void allocNextBlock() {
            final MemoryBlock nextMb = MemoryBlock.allocHeapBlock();
            blocks.add(nextMb);
            currentBlock = nextMb;
            currentBlockOffset = nextMb.offset;
            areaSize += nextMb.length;
        }
    }

    // ---------------------------------------------------

    public static void main(final String[] args) {

        final byte[] b0 = new byte[4096-123];

        final byte[] b1 = new byte[4096-123];

        final SerializedArray<byte[]> arr = new SerializedArray<>(5);

        arr.store(0, b0);

        arr.store(1, b1);

        final byte[] bf = arr.fetch(1);


        final byte[] b2 = new byte[4096];

        arr.store(0, b2);
    }

    // ---------------------------------------------------

    private static class Dummy implements Serializable {
        public Dummy() {}
        private int i       = 1;
        private double e    = 1232.2130;
        private int u       = 12;
    }

    private static final int NR_BITS = Integer.valueOf(System.getProperty("sun.arch.data.model"));
    private static final int BYTE = 8;
    private static final int WORD = NR_BITS/BYTE;
    private static final int MIN_SIZE = 16;

    public static int sizeOf(Class src){
        final List<Field> instanceFields = new LinkedList<>();
        do {
            if (src == Object.class) return MIN_SIZE;
            for (Field f : src.getDeclaredFields()) {
                if ((f.getModifiers() & Modifier.STATIC) == 0) {
                    instanceFields.add(f);
                }
            }
            src = src.getSuperclass();
        } while (instanceFields.isEmpty());
        long maxOffset = 0;
        for (Field f : instanceFields) {
            long offset = UnsafeOp.unsafe.objectFieldOffset(f);
            if(offset > maxOffset) maxOffset = offset;
        }
        return  (((int) maxOffset / WORD) + 1) * WORD;
    }

    // ---------------------------------------------------

    public static int hasObjectConstantSize(final Class<?> clazz) {
        Preconditions.checkNotNull(clazz);
        Class<?> src = clazz;
        final List<Field> instanceFields = new LinkedList<>();
        do {
            //if (src == Object.class) return MIN_SIZE;
            for (Field f : src.getDeclaredFields()) {
                if ((f.getModifiers() & Modifier.STATIC) == 0) {
                    instanceFields.add(f);
                }
            }
            src = src.getSuperclass();
        } while (instanceFields.isEmpty());
        //long maxOffset = 0;
        int size = 0;
        for (final Field field : instanceFields) {
            final Class<?> fieldClass = field.getType();
            if (fieldClass.isPrimitive()) {
                if (fieldClass == boolean.class)  size += 4;
                if (fieldClass == byte.class)     size += 1;
                if (fieldClass == short.class)    size += 2;
                if (fieldClass == int.class)      size += 4;
                if (fieldClass == long.class)     size += 8;
                if (fieldClass == float.class)    size += 4;
                if (fieldClass == double.class)   size += 8;
            } else
                size += hasObjectConstantSize(clazz);
            //long offset = UnsafeOp.unsafe.objectFieldOffset(field);
            //if(offset > maxOffset) maxOffset = offset;
        }
        //return  (((int) maxOffset / WORD) + 1) * WORD;
        return size;
    }

    // ---------------------------------------------------
}
