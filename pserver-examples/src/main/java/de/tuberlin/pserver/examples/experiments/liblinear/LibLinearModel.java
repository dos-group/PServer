package de.tuberlin.pserver.examples.experiments.liblinear;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.matrix.Matrix;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class LibLinearModel implements Serializable {

    private final Parameter param;

    private Matrix labels;

    private int nrClass;

    public List<GeneralizedLinearModel> subModels;

    public double bias      = -1.0;

    public double threshold = 0.0;

    public LibLinearModel(final Parameter param, final Matrix labels) {

        this.param  = Preconditions.checkNotNull(param);

        this.labels = Preconditions.checkNotNull(labels);

        this.subModels = new ArrayList<>();
    }
}
