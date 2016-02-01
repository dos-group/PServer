package de.tuberlin.pserver.matrix.crdt;

import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.Matrix64F;

public interface AvgMatrix32F extends Matrix32F {

    void includeInAvg(long row, long col, Float value);
}
