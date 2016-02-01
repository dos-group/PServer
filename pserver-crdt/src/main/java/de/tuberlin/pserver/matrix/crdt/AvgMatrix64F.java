package de.tuberlin.pserver.matrix.crdt;

import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix64F;

public interface AvgMatrix64F extends Matrix64F {

    void includeInAvg(long row, long col, Double value);
}
