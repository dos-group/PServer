package de.tuberlin.pserver.ml.models;


import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import de.tuberlin.pserver.app.PServerContext;
import de.tuberlin.pserver.utils.GsonUtils;

import java.io.Serializable;

public abstract class Model<T> implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected transient static Gson gson = GsonUtils.createPrettyPrintAndAnnotationExclusionGson();

    public final String name;

    public final int instanceID;

    private long startTrainingTime;

    private long stopTrainingTime;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public Model(final String name, final int instanceID) {

        this.name       = Preconditions.checkNotNull(name);

        this.instanceID = instanceID;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void createModel(final PServerContext ctx);

    public abstract void fetchModel(final PServerContext ctx);

    public abstract T copy();

    // ---------------------------------------------------

    public void startTraining() { startTrainingTime = System.currentTimeMillis(); }

    public void stopTraining() { stopTrainingTime = System.currentTimeMillis(); }

    public long getTrainingTime() { return stopTrainingTime - startTrainingTime; }
}
