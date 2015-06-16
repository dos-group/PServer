package de.tuberlin.pserver.math.delegates;

import de.tuberlin.pserver.math.*;

/**
 * Created by fsander on 02.06.15.
 */
public abstract class DelegationMatrix<T> extends AbstractMatrix {

    protected T target;

    public T getTarget() {
        return target;
    }

    public DelegationMatrix(long rows, long cols, T target) {
        super(rows, cols);
        this.target = target;
    }

    @Override
    public double atomicGet(long row, long col) {
        synchronized (target) {
            return get(row, col);
        }
    }

    @Override
    public void atomicSet(long row, long col, double value) {
        synchronized (target) {
            set(row, col, value);
        }
    }

}
