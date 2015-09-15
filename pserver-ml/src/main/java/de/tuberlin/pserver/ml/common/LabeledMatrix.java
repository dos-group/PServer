package de.tuberlin.pserver.ml.common;


import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import de.tuberlin.pserver.commons.json.GsonUtils;
import de.tuberlin.pserver.math.matrix.Matrix;

import java.io.Serializable;

public class LabeledMatrix implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private transient static Gson gson = GsonUtils.createPrettyPrintAndAnnotationExclusionGson();

    public final double label;

    public final Matrix matrix;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public LabeledMatrix(final double label, final Matrix matrix) {
        this.label  = label;
        this.matrix = Preconditions.checkNotNull(matrix);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;
        if (obj.getClass() != getClass()) return false;
        return ((LabeledMatrix)obj).label == label
                && matrix.equals(((LabeledMatrix)obj).matrix);
    }

    @Override
    public String toString() {
        return "LabeledMatrix" + gson.toJson(this);
    }
}
