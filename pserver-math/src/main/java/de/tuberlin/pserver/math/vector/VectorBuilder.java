package de.tuberlin.pserver.math.vector;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.vector.dense.DVector;
import de.tuberlin.pserver.math.vector.sparse.SVector;

public class VectorBuilder {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private long length;

    private Format format;

    private Layout layout;

    private boolean mutable;

    private double[] data;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public VectorBuilder() {
        reset();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public VectorBuilder dimension(final long length) {
        this.length = length;
        return this;
    }

    public VectorBuilder format(final Format format) {
        this.format = Preconditions.checkNotNull(format);
        return this;
    }

    public VectorBuilder layout(final Layout layout) {
        this.layout = Preconditions.checkNotNull(layout);
        return this;
    }

    public VectorBuilder mutable(final boolean mutable) {
        this.mutable = mutable;
        return this;
    }

    public VectorBuilder data(final double[] data) {
        this.data = Preconditions.checkNotNull(data);
        return this;
    }

    public Vector build() {
        switch (format) {
            case SPARSE_FORMAT:
                if (mutable)
                    return new SVector(length, layout);
                else
                    throw new UnsupportedOperationException("");
            case DENSE_FORMAT:
                if (mutable)
                    return new DVector(length, data, layout);
                else
                    throw new UnsupportedOperationException("");
        }
        throw new IllegalStateException();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    private void reset() {
        length   = -1;
        format  = Format.SPARSE_FORMAT;
        layout  = Layout.ROW_LAYOUT;
        mutable = true;
    }
}
