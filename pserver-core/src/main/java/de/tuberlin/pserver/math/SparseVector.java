package de.tuberlin.pserver.math;


import de.tuberlin.pserver.core.memory.Buffer;
import de.tuberlin.pserver.utils.nbhm.NonBlockingHashMap;

import java.util.concurrent.ConcurrentMap;

public class SparseVector implements IVector {

    private final ConcurrentMap<Integer, Buffer> data = new NonBlockingHashMap<>();

}
