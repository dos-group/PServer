package de.tuberlin.pserver.math;

import de.tuberlin.pserver.utils.nbhm.NonBlockingHashMap;

import java.util.concurrent.ConcurrentMap;

public class SparseMatrix implements IMatrix {

    private final ConcurrentMap<Integer, SparseVector> data = new NonBlockingHashMap<>();
}
