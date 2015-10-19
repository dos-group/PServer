package de.tuberlin.pserver.examples.experiments.tsne;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.phases.Apply;
import de.tuberlin.pserver.dsl.transaction.phases.Prepare;
import de.tuberlin.pserver.dsl.transaction.properties.TransactionType;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix64F;
import de.tuberlin.pserver.math.matrix.partitioning.PartitionShape;
import de.tuberlin.pserver.math.tuples.Tuple2;
import de.tuberlin.pserver.runtime.mcruntime.Parallel;
import de.tuberlin.pserver.types.DistributedMatrix64F;
import de.tuberlin.pserver.utils.MatrixBuilder;
import org.apache.commons.lang3.mutable.MutableDouble;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.List;

public class TSNEJob extends Program {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final int ROWS = 10;
    private static final int INPUT_COLS = 28 * 28;
    private static final int EMBEDDING_DIMENSION = 2;

    private static final int NUM_EPOCHS = 1;
    private static final double PERPLEXITY = 2.0;
    private static final double LEARNING_RATE = 350.0;
    private static final double EARLY_EXAGGERATION = 1.0;

    private static final double INITIAL_MOMENTUM = 0.5;
    private static final double FINAL_MOMENTUM = 0.8;
    private static final double MIN_GAIN = 0.01;
    private static final double TOL = 1e-5;

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    // input. i.e. activation vectors of a neuronal network
    @State(scope = Scope.REPLICATED, rows = ROWS, cols = INPUT_COLS, path = "datasets/mnist_10_X.csv")
    public Matrix64F X;

    // high dimensional affinity function (for two vectors of input space)
    @State(scope = Scope.PARTITIONED, rows = ROWS, cols = ROWS)
    public Matrix64F P;

    // model. linear embedding.
    @State(scope = Scope.REPLICATED,
           rows = ROWS, cols = EMBEDDING_DIMENSION,
           path = "datasets/mnist_10_initY.csv")

    public Matrix64F Y;

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    @Transaction(state = "Y", type = TransactionType.PULL)
    public TransactionDefinition syncY = new TransactionDefinition(

            (Prepare<Matrix, Matrix>) object -> null,

            (Apply<Matrix, Void>) object -> null
    );

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit
    public void main(final Lifecycle lifecycle) {

        lifecycle.process(() -> {
            // calc affinity. P is affinity for input X
            P.assign(binarySearch(X, TOL, PERPLEXITY));
            // symmetrize
            Matrix64F PT = P.transpose();
            P.add(PT, P);
            // keep pdf properties
            double sumP = P.aggregateRows(Matrix<Double>::sum).sum();
            P.scale(1 / sumP, P);
            // early exaggeration
            P.scale(EARLY_EXAGGERATION, P);

            final long n = Y.rows();
            final long d = Y.cols();

            // Q is affinity for model Y
            final Matrix64F Q = new MatrixBuilder().dimension(n, n).build();
            // for calc of Q
            final Matrix64F Y_squared = new MatrixBuilder().dimension(n, d).build();
            // strengthen good direction, weaken bad ones (in gradient descent)
            final Matrix64F gains = new MatrixBuilder().dimension(n, d).build();
            // previous gradient. moment of direction "movement"
            final Matrix64F iY = new MatrixBuilder().dimension(n, d).build();
            // current gradient
            final Matrix64F dY = new MatrixBuilder().dimension(n, d).build();
            // need to center Y in each iteration. define reusable matrix here
            final Matrix64F mean = new MatrixBuilder().dimension(1, d).build();

            gains.assign(1.0);

            // annealing factor
            final MutableDouble momentum = new MutableDouble(0.0);

            UnitMng.loop(NUM_EPOCHS, (epoch) -> {

                // calc distance matrix of Y
                Y.applyOnElements(e -> Math.pow(e, 2), Y_squared);
                final Matrix64F sum_Y = Y_squared.aggregateRows(Matrix<Double>::sum);
                final Matrix64F num = Y.mul(Y.transpose())
                        .scale(-2.)
                        .addVectorToRows(sum_Y)
                        .transpose()
                        .addVectorToRows(sum_Y)
                        .applyOnElements(e -> 1.0 / (e + 1.0));
                // should be that way. but who knows...
                num.setDiagonalsToZero(num);
                // num for Y is the same D for X
                // its the distance matrix for Y...

                // ---------------------------------------------------
                // (2) SINGLETON OPERATION!!!
                // ---------------------------------------------------
                final double sumNum = num.aggregateRows(Matrix<Double>::sum).sum();

                num.scale(1. / sumNum, Q);
                Q.applyOnElements(e -> Math.max(e, 1e-12), Q);

                //final Matrix PQ = P.sub(Q);

                PartitionShape shape = ((DistributedMatrix64F)P).getPartitionShape();
                Parallel.For(shape.rows, (i) -> {
                    // TODO: get target vector of dY instead. possible? or resuable?
                    final Matrix64F sumVec = new MatrixBuilder().dimension(1, d).build();
                    UnitMng.loop(P.cols(), (j) -> {
                        final Double pq = P.get(shape.rowOffset + i, j) - Q.get(i, j);//PQ.get(i, j);
                        final Double num_j = num.get(i, j);
                        sumVec.add(
                                Y.getRow(i).sub(Y.getRow(j)) // yi - yj
                                        .scale(pq * num_j),
                                sumVec);
                    });
                    dY.assignRow(shape.rowOffset + i, sumVec);
                });

                momentum.setValue((epoch < 20) ? INITIAL_MOMENTUM : FINAL_MOMENTUM);

                // set gain
                Parallel.For(gains, (i, j, v) -> {
                    final Double dY_j = dY.get(i, j);
                    final Double iY_j = iY.get(i, j);
                    final Double gain_j = gains.get(i, j);
                    // same as in reference. but why?
                    gains.set(i, j, (dY_j > 0) == (iY_j > 0) ? gain_j * 0.8 : gain_j + 0.2);
                });

                gains.applyOnElements(e -> Math.max(e, MIN_GAIN), gains);

                // calc new gradient and apply
                iY.scale(momentum.getValue()).sub(dY.applyOnElements(gains, (e1, e2) -> e1 * e2).scale(LEARNING_RATE), iY);
                Y.add(iY, Y);

                UnitMng.barrier(UnitMng.GLOBAL_BARRIER);
                TransactionMng.commit(syncY);

                // center Y
                mean.assign(0.0);
                for (int i = 0; i < Y.rows(); ++i)
                    mean.add(Y.getRow(i), mean);
                mean.scale(-1. / Y.rows(), mean);
                Y.addVectorToRows(mean, Y);

                if ((epoch + 1) % 10 == 0) {
                    double C = 0.0;
                    for (int i = 0; i < P.rows(); ++i) {
                        for (int j = 0; j < P.cols(); ++j) {
                            if (i != j) {
                                C += P.get(i, j) * Math.log(P.get(i, j) / Q.get(i, j));
                            }
                        }
                    }
                    LOG.info("Iteration " + (epoch + 1) + ", Error: " + C);
                }

                if (epoch == 100) {
                    P.scale(1.0 / EARLY_EXAGGERATION, P);
                }

                LOG.debug("Y: " + Y.getRow(0).toString());

            });

        }).postProcess(() -> result(Y));
    }

    // ---------------------------------------------------
    // Helper Methods.
    // ---------------------------------------------------

    private Matrix64F binarySearch(Matrix64F X, Double tol, Double perplexity) {

        PartitionShape shape = ((DistributedMatrix64F)P).getPartitionShape();

        long n = shape.rows;

        final Matrix64F P_tmp      = new MatrixBuilder().dimension(shape.rows, shape.cols).build();
        // beta = 1/(2*sigma^2). sigma is depended on a point. so we have n sigmas
        final Matrix64F beta       = new MatrixBuilder().dimension(1, n).build();

        beta.assign(1.);

        final Matrix64F sumX = X.applyOnElements(e -> Math.pow(e, 2)).aggregateRows(Matrix<Double>::sum);
        // X * X^. Then we have N x N. scale with -2. Add sumX row-wise and col-wise.
        // distance matrix of X
        final Matrix64F D = X.mul(X.transpose()).scale(-2.).addVectorToRows(sumX).transpose().addVectorToRows(sumX);

        final double logU = (float)Math.log(perplexity);

        for (long i=0; i < n; ++i) {

            double betaMin = Double.NEGATIVE_INFINITY;
            double betaMax = Double.POSITIVE_INFINITY;

            final Matrix64F Di = D.getRow(shape.rowOffset + i);
            Tuple2<Double, Matrix64F> hBeta = computeHBeta(Di, beta.get(i), i);

            double H = hBeta._1;
            Matrix64F Pi = hBeta._2;
            // Evaluate whether the perplexity is within tolerance

            double HDiff = H - logU;
            int tries = 0;

            while(Math.abs(HDiff) > tol && tries++ < 50){

                if (HDiff > 0){
                    betaMin = beta.get(0, i);
                    if (Double.isInfinite(betaMax))
                        beta.set(0, i, beta.get(i) * 2);
                    else
                        beta.set(0, i, (beta.get(i) + betaMax) / 2);
                } else {
                    betaMax = beta.get(0, i);
                    if (Double.isInfinite(betaMin))
                        beta.set(0, i, beta.get(i) / 2);
                    else
                        beta.set(0, i, (beta.get(i) + betaMin) / 2);
                }

                hBeta = computeHBeta(Di, beta.get(i), i);
                H = hBeta._1;
                Pi = hBeta._2;
                HDiff = H - logU;
            }
            P_tmp.assignRow(i, Pi);
        }

        return P_tmp;
    }

    private Tuple2<Double, Matrix64F> computeHBeta(final Matrix64F d, final double beta, final long index){
        final Matrix64F P = new MatrixBuilder().dimension(1, d.cols()).build();
        P.set(0, index, 0.);

        // parExe over all elements i != j
        for (long i=0; i < P.cols(); ++i) {
            if (i != index) {
                P.set(0, i, Math.exp(-1 * d.get(i) * beta));
            }
        }

        final Matrix64F PD = P.applyOnElements(d, (e1, e2) -> e1 * e2);
        final double sumP = P.sum();
        final double H = (float)Math.log(sumP) + beta * PD.sum() / sumP;

        P.applyOnElements(e -> e / sumP, P);
        return new Tuple2<>(H, P);
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void cluster() {
        System.setProperty("pserver.profile", "wally");
        PServerExecutor.REMOTE
                .run(TSNEJob.class)
                .done();
    }

    public static void local() {
        System.setProperty("simulation.numNodes", "2");

        final List<List<Serializable>> res = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(TSNEJob.class)
                .results(res)
                .done();

        try (PrintWriter writer = new PrintWriter("datasets/pserver_mnist.csv", "UTF-8")) {

            Matrix64F R = (Matrix64F) res.get(0).get(0);
            for (int i = 0; i < R.rows(); ++i) {
                for (int j = 0; j < R.cols(); ++j) {
                    writer.println(i + "," + j + "," + R.get(i, j));
                }
            }

        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    public static void main(final String[] args) {
        local();
    }
}