package de.tuberlin.pserver.examples.experiments.liblinear;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.compiler.ProgramContext;
import de.tuberlin.pserver.dsl.transaction.aggregators.Aggregator;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.dense.Dense64Matrix;
import de.tuberlin.pserver.runtime.mcruntime.shared.SharedInt;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;

// TODO: need primitive to compute at one slot and use result at all.

// TODO: nonZeroExe iterator

// TODO: tValue - tail value


public final class LibLinearSolver {

    private final ProgramContext pc;

    public LibLinearSolver(final ProgramContext pc) {

        this.pc = Preconditions.checkNotNull(pc);
    }

    private GeneralizedLinearModel train_one(final Problem prob, final Parameter param, final double posLabel) throws Exception {

        Matrix w = null;

        Problem binaryProb = prob.genBinaryProblem(posLabel);

        SharedInt count = new SharedInt(pc, 0);
        /*sc.CF.loop().parExe(binaryProb.dataPoints, (e, it) -> {
            if (it.value(it.cols() - 1) > 0)
                count.inc();
        });*/

        int pos = new Aggregator<>(pc, count.done().get())
                .apply(c -> c.stream()
                                .mapToInt(Integer::intValue)
                                .sum()
                );

        long neg = binaryProb.l - pos;
        double primalSolverTol = param.eps * Math.max(Math.min(pos, neg), 1) / binaryProb.l;

        switch(param.solverType) {
            case L2_LR:
                break;
            case UNKNOWN:
                break;
        }

        return null;
    }

    /*private def train_one(prob : Problem, param : Parameter, posLabel : Double) : GeneralizedLinearModel =
    {
        var w : DoubleMatrix = null
		// Construct binary labels.
        val binaryProb = prob.genBinaryProb(posLabel)

        val pos = binaryProb.dataPoints.map(point => point.y).filter(_ > 0).count()
        val neg = binaryProb.l - pos
        val primalSolverTol = param.eps * max(min(pos,neg), 1)/binaryProb.l;

        param.solverType match {
        case L2_LR => {
            var solver = new Tron(new TronLR())
            w = solver.tron(binaryProb, param, primalSolverTol)
        }
        case L2_L2LOSS_SVC => {
            var solver = new Tron(new TronL2SVM())
            w = solver.tron(binaryProb, param, primalSolverTol)
        }
        case _ => {
            System.err.println("ERROR: unknown solver_type")
            return null
        }
    }
        binaryProb.dataPoints.unpersist()
        var intercept = 0.0
        var weights : Array[Double] = null
        if (prob.bias < 0)
        {
            weights = w.toArray()
        }
        else
        {
            weights = w.toArray.slice(0,w.length - 1)
            intercept = w.get(w.length - 1)
        }

        param.solverType match {
        case L2_LR => {
            var model = new LogisticRegressionModel(Vectors.dense(weights), intercept)
            model.clearThreshold()
            model
        }
        case L2_L2LOSS_SVC => {
            var model = new SVMModel(Vectors.dense(weights), intercept)
            model.clearThreshold()
            model
        }
    }
    }*/

    private LibLinearModel train(final Problem prob, final Parameter param) throws Exception {

        final Matrix dp = prob.dataPoints;

        final double[] labels = dp.getCol(dp.cols() - 1).toArray();

        final double ls[] = Arrays.asList(ArrayUtils.toObject(labels))
                .stream().distinct().mapToDouble(Double::doubleValue).toArray();

        final Matrix partialLabelSet = new Dense64Matrix(1, ls.length, ls);

        final Matrix labelSet = new Aggregator<>(pc, partialLabelSet).apply(partialAggs -> {
            Matrix result = partialAggs.size() > 0 ? partialAggs.get(0) : new Dense64Matrix(1, 1);
            for (int i = 1; i < partialAggs.size(); i++) {
                result.concat(partialAggs.get(i), result);
            }
            return result;
        });

        final LibLinearModel model = new LibLinearModel(param, labelSet);

        model.bias = prob.bias;

        for (int i = 0; i < labelSet.cols(); ++i) {
            model.subModels.add(train_one(prob, param, labelSet.get(i)));
        }

        if(param.solverType == SolverType.L2_LR)
            model.threshold = 0.5;

        return model;
    }
}
