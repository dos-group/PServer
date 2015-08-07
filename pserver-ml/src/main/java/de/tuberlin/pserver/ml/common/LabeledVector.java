package de.tuberlin.pserver.ml.common;


import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import de.tuberlin.pserver.commons.json.GsonUtils;
import de.tuberlin.pserver.math.vector.Vector;

import java.io.Serializable;

public class LabeledVector implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private transient static Gson gson = GsonUtils.createPrettyPrintAndAnnotationExclusionGson();

    public final double label;

    public final Vector vector;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public LabeledVector(final double label, final Vector vector) {
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
