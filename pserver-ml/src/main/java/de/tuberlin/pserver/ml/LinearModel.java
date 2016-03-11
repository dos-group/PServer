package de.tuberlin.pserver.ml;


import de.tuberlin.pserver.commons.serialization.LocalFSObjectStorage;
import de.tuberlin.pserver.types.matrix.MatrixBuilder;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.dense.DenseMatrix32F;

import java.util.List;
import java.util.concurrent.CyclicBarrier;

public final class LinearModel {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final DenseMatrix32F model;

    // ---------------------------------------------------

    transient private final DenseMatrix32F internal[];

    // ---------------------------------------------------

    transient private final int dop;

    transient private final String path;

    transient private final CyclicBarrier barrier;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public LinearModel() { this(0, null, null); }
    public LinearModel(int dop, DenseMatrix32F model, String path) {
        this.dop        = dop;
        this.model      = model;
        this.path       = path;

        this.internal   = new DenseMatrix32F[dop];
        for (int i = 0; i < dop; ++i) {
            internal[i]   = (DenseMatrix32F)new MatrixBuilder().dimension(1, model.cols()).build();
        }

        this.barrier  = new CyclicBarrier(dop, () -> {
            for (int i = 0; i < dop; ++i)
                model.add(internal[i], model);
            model.scale(1.0f / dop);
        });
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public DenseMatrix32F getModel(int id) { return internal[id]; }

    // ---------------------------------------------------

    public DenseMatrix32F getModel() { return model; }

    // ---------------------------------------------------

    public void save() { LocalFSObjectStorage.writeTo(this, path); }

    public void save(long epoch) { LocalFSObjectStorage.writeTo(this, path + "_" + epoch); }

    // ---------------------------------------------------

    public LinearModel nextEpoch() {
        for (int i = 0; i < dop; ++i)
            System.arraycopy(model.data, 0, internal[i].data, 0, model.data.length);
        return this;
    }

    public LinearModel merge(List<Matrix32F> models) throws Exception {
        for (Matrix32F m : models)
            model.add(m, model);
        model.scale(1.0f / models.size(), model);
        return this;
    }

    public LinearModel sync() throws Exception {
        barrier.await();
        return this;
    }
}
