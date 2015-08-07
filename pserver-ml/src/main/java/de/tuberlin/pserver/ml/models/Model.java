package de.tuberlin.pserver.ml.models;


import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import de.tuberlin.pserver.commons.json.GsonUtils;
import de.tuberlin.pserver.runtime.InstanceContext;

import java.io.Serializable;

public abstract class Model<T> implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected transient static Gson gson = GsonUtils.createPrettyPrintAndAnnotationExclusionGson();

    public final String name;

    public final int nodeID;

    private long startTrainingTime;

    private long stopTrainingTime;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public Model(final String name, final int nodeID) {

        this.name       = Preconditions.checkNotNull(name);

        this.nodeID = nodeID;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void createModel(final InstanceContext ctx);

    public abstract void fetchModel(final InstanceContext ctx);

    public abstract T copy();

    // ---------------------------------------------------

    public void startTraining() { startTrainingTime = System.currentTimeMillis(); }

    public void stopTraining() { stopTrainingTime = System.currentTimeMillis(); }

    public long getTrainingTime() { return stopTrainingTime - startTrainingTime; }
}
