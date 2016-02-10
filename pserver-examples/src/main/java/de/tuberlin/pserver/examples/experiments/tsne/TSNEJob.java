package de.tuberlin.pserver.examples.experiments.tsne;
/*
import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.annotations.TransactionType;
import de.tuberlin.pserver.dsl.transaction.phases.Apply;
import de.tuberlin.pserver.dsl.transaction.phases.Prepare;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix32.Matrix32;
import de.tuberlin.pserver.math.matrix32.partitioner.PartitionShape;
import de.tuberlin.pserver.math.tuples.Tuple2;
import de.tuberlin.pserver.runtime.parallel.Parallel;
import de.tuberlin.pserver.math.matrix32.MatrixBuilder;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableFloat;

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
    private static final float PERPLEXITY = 2.0f;
    private static final float LEARNING_RATE = 350.0f;
    private static final float EARLY_EXAGGERATION = 1.0f;

    private static final float INITIAL_MOMENTUM = 0.5f;
    private static final float FINAL_MOMENTUM = 0.8f;
    private static final float MIN_GAIN = 0.01f;
    private static final float TOL = 1e-5f;

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    // input. i.e. activation vectors of a neuronal network
    @State(scope = Scope.REPLICATED, rows = ROWS, cols = INPUT_COLS, path = "datasets/mnist_10_X.csv")
    public Matrix32 X;

    // high dimensional affinity function (for two vectors of input space)
    @State(scope = Scope.PARTITIONED, rows = ROWS, cols = ROWS)
    public Matrix32 P;

    // model. linear embedding.
    @State(scope = Scope.REPLICATED,
           rows = ROWS, cols = EMBEDDING_DIMENSION,
           path = "datasets/mnist_10_initY.csv")

    public Matrix32 Y;

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    @Transaction(state = "Y", type = TransactionType.PULL)
    public TransactionDefinition syncY = new TransactionDefinition(

            (Prepare<Matrix32, Matrix32>) (requestObj, object) -> null,

            (Apply<Matrix32, Void>) (requestObj, remote, local) -> null
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
            Matrix32 PT = P.transpose();
            P.add(PT, P);
            // keep pdf properties
            float sumP = P.aggregateRows(Matrix32::sum).sum();
            P.scale(1 / sumP, P);
            // early exaggeration
            P.scale(EARLY_EXAGGERATION, P);

            final long n = Y.rows();
            final long d = Y.cols();

            // Q is affinity for model Y
            final Matrix32 Q = new MatrixBuilder().dimension(n, n).build();
            // for calc of Q
            final Matrix32 Y_squared = new MatrixBuilder().dimension(n, d).build();
            // strengthen good direction, weaken bad ones (in gradient descent)
            final Matrix32 gains = new MatrixBuilder().dimension(n, d).build();
            // previous gradient. moment of direction "movement"
            final Matrix32 iY = new MatrixBuilder().dimension(n, d).build();
            // current gradient
            final Matrix32 dY = new MatrixBuilder().dimension(n, d).build();
            // need to center Y in each iteration. define reusable matrix here
            final Matrix32 mean = new MatrixBuilder().dimension(1, d).build();

            gains.assign(1.0f);

            // annealing factor
            final MutableFloat momentum = new MutableFloat(0.0f);

            UnitMng.loop(NUM_EPOCHS, (epoch) -> {

                // calc distance matrix of Y
                Y.applyOnElements(e -> (float)Math.pow(e, 2), Y_squared);
                final Matrix32 sum_Y = Y_squared.aggregateRows(Matrix32::sum);
                final Matrix32 num = Y.mul(Y.transpose())
                        .scale(-2.f)
                        .addVectorToRows(sum_Y)
                        .transpose()
                        .addVectorToRows(sum_Y)
                        .applyOnElements(e -> 1.0f / (e + 1.0f));
                // should be that way. but who knows...
                num.setDiagonalsToZero(num);
                // num for Y is the same D for X
                // its the distance matrix for Y...

                // ---------------------------------------------------
                // (2) SINGLETON OPERATION!!!
                // ---------------------------------------------------
                final float sumNum = num.aggregateRows(Matrix32::sum).sum();

                num.scale(1.f / sumNum, Q);
                Q.applyOnElements(e -> (float)Math.max(e, 1e-12), Q);

                //final Matrix PQ = P.sub(Q);

                PartitionShape shape = P.getPartitionShape();
                Parallel.For(shape.rows, (i) -> {
                    // TODO: get target vector of dY instead. possible? or resuable?
                    final Matrix32 sumVec = new MatrixBuilder().dimension(1, d).build();
                    UnitMng.loop(P.cols(), (j) -> {
                        final float pq = P.get(shape.rowOffset + i, j) - Q.get(i, j);//PQ.get(i, j);
                        final float num_j = num.get(i, j);
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
                    final float dY_j = dY.get(i, j);
                    final float iY_j = iY.get(i, j);
                    final float gain_j = gains.get(i, j);
                    // same as in reference. but why?
                    gains.set(i, j, (dY_j > 0) == (iY_j > 0) ? gain_j * 0.8f : gain_j + 0.2f);
                });

                gains.applyOnElements(e -> Math.max(e, MIN_GAIN), gains);

                // calc new gradient and apply
                iY.scale((float)momentum.doubleValue()).sub(dY.applyOnElements(gains, (e1, e2) -> e1 * e2).scale(LEARNING_RATE), iY);
                Y.add(iY, Y);

                UnitMng.barrier(UnitMng.GLOBAL_BARRIER);
                TransactionMng.commit(syncY);

                // center Y
                mean.assign(0.0f);
                for (int i = 0; i < Y.rows(); ++i)
                    mean.add(Y.getRow(i), mean);
                mean.scale(-1.f / Y.rows(), mean);
                Y.addVectorToRows(mean, Y);

                if ((epoch + 1) % 10 == 0) {
                    float C = 0.0f;
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
                    P.scale(1.0f / EARLY_EXAGGERATION, P);
                }

                LOG.debug("Y: " + Y.getRow(0).toString());

            });

        }).postProcess(() -> result(Y));
    }

    // ---------------------------------------------------
    // Helper Methods.
    // ---------------------------------------------------

    private Matrix32 binarySearch(Matrix32 X, float tol, float perplexity) {

        PartitionShape shape = P.getPartitionShape();

        long n = shape.rows;

        final Matrix32 P_tmp      = new MatrixBuilder().dimension(shape.rows, shape.cols).build();
        // beta = 1/(2*sigma^2). sigma is depended on a point. so we have n sigmas
        final Matrix32 beta       = new MatrixBuilder().dimension(1, n).build();

        beta.assign(1.f);

        final Matrix32 sumX = X.applyOnElements(e -> (float)Math.pow(e, 2)).aggregateRows(Matrix32::sum);
        // X * X^. Then we have N x N. scale with -2. Add sumX row-wise and col-wise.
        // distance matrix of X
        final Matrix32 D = X.mul(X.transpose()).scale(-2.f).addVectorToRows(sumX).transpose().addVectorToRows(sumX);

        final float logU = (float)Math.log(perplexity);

        for (long i=0; i < n; ++i) {

            float betaMin = Float.NEGATIVE_INFINITY;
            float betaMax = Float.POSITIVE_INFINITY;

            final Matrix32 Di = D.getRow(shape.rowOffset + i);
            Tuple2<Float, Matrix32> hBeta = computeHBeta(Di, beta.get(i), i);

            float H = hBeta._1;
            Matrix32 Pi = hBeta._2;
            // Evaluate whether the perplexity is within tolerance

            float HDiff = H - logU;
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

    private Tuple2<Float, Matrix32> computeHBeta(final Matrix32 d, final float beta, final long index){
        final Matrix32 P = new MatrixBuilder().dimension(1, d.cols()).build();
        P.set(0, index, 0.f);

        // parExe over all elements i != j
        for (long i=0; i < P.cols(); ++i) {
            if (i != index) {
                P.set(0, i, (float)Math.exp(-1 * d.get(i) * beta));
            }
        }

        final Matrix32 PD = P.applyOnElements(d, (e1, e2) -> e1 * e2);
        final float sumP = P.sum();
        final float H = (float)Math.log(sumP) + beta * PD.sum() / sumP;

        P.applyOnElements(e -> e / sumP, P);
        return new Tuple2<>(H, P);
    }

    // ---------------------------------------------------
    // EntryImpl Point.
    // ---------------------------------------------------

    public static void main(final String[] args) { local(); }

    // ---------------------------------------------------

    private static void cluster() {
        System.setProperty("pserver.profile", "wally");
        PServerExecutor.REMOTE
                .run(TSNEJob.class)
                .done();
    }

    private static void local() {
        System.setProperty("simulation.numNodes", "1");

        final List<List<Serializable>> res = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(TSNEJob.class)
                .results(res)
                .done();

        try (PrintWriter writer = new PrintWriter("datasets/pserver_mnist.csv", "UTF-8")) {

            Matrix32 R = (Matrix32) res.get(0).get(0);
            for (int i = 0; i < R.rows(); ++i) {
                for (int j = 0; j < R.cols(); ++j) {
                    writer.println(i + "," + j + "," + R.get(i, j));
                }
            }

        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }
}*/