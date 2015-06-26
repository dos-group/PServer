package de.tuberlin.pserver.examples.playground;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.ml.algorithms.linreg.LinRegAlgorithm;
import de.tuberlin.pserver.ml.algorithms.linreg.LinRegModel;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;

public final class LinRegTestJob extends PServerJob {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private LinRegModel model = new LinRegModel("model1", 4);;

    private LinRegAlgorithm linreg;

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {
        dataManager.loadDMatrix("datasets/demo_dataset.csv");
        model.createModel(ctx);
    }

    @Override
    public void compute() {
        final Matrix data = dataManager.getLocalMatrix("demo_dataset.csv");
        linreg = new LinRegAlgorithm(ctx);
        linreg.register();
        linreg.train(model, data);
        linreg.unregister();
        result(model.getWeights());
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final List<List<Serializable>> res = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(LinRegTestJob.class, 4)
                .results(res)
                .done();

        final DecimalFormat numberFormat = new DecimalFormat("0.000");
        res.forEach(
                r -> r.forEach(
                        w -> {
                            for (double weight : ((Matrix)w).toArray())
                                System.out.print(numberFormat.format(weight) + "\t | ");
                            System.out.println();
                        }
                )
        );
    }
}