package de.tuberlin.pserver.ml.playground.optimization2;


import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import de.tuberlin.pserver.utils.GsonUtils;

import java.io.Serializable;

public class LabeledVector<TLabel, TFeature> implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private transient static Gson gson = GsonUtils.createPrettyPrintAndAnnotationExclusionGson();

    public final TLabel label;

    public final TFeature vector;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public LabeledVector(final TLabel label, final TFeature vector) {
        this.label  = label;
        this.vector = Preconditions.checkNotNull(vector);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;
        if (obj.getClass() != getClass()) return false;
        return ((LabeledVector)obj).label == label
                && vector.equals(((LabeledVector)obj).vector);
    }

    @Override
    public String toString() {
        return "LabeledVector" + gson.toJson(this);
    }
}
