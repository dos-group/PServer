package de.tuberlin.pserver.math.delegates.sparse.mtj;

import de.tuberlin.pserver.math.SVector;
import de.tuberlin.pserver.math.Utils;
import de.tuberlin.pserver.math.Vector;

public class MTJVector extends SVector<no.uib.cipr.matrix.Vector> {

    public MTJVector(long size, VectorType type, no.uib.cipr.matrix.Vector target) {
        super(size, type, target);
    }

    @Override
    public boolean isDense() {
        return MTJUtils.isDense(target);
    }

    @Override
    public void set(long index, double value) {
        target.set(Utils.toInt(index), value);
    }

    @Override
    public double get(long index) {
        return target.get(Utils.toInt(index));
    }

    @Override
    public Vector viewPart(long s, long e) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public Vector like() {
        return new MTJVector(size, type, target.copy());
    }
}
