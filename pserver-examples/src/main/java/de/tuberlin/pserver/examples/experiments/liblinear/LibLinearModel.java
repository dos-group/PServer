package de.tuberlin.pserver.examples.experiments.liblinear;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.vector.Vector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class LibLinearModel implements Serializable {

    private final Parameter param;

    private Vector labels;

    private int nrClass;

    public List<GeneralizedLinearModel> subModels;

    public double bias      = -1.0;

    public double threshold = 0.0;

    public LibLinearModel(final Parameter param, final Vector labels) {

        this.param  = Preconditions.checkNotNull(param);

        this.labels = Preconditions.checkNotNull(labels);

        this.subModels = new ArrayList<>();
    }
}
