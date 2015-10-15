package de.tuberlin.pserver.examples.experiments.tsne;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.partitioning.PartitionShape;
import de.tuberlin.pserver.utils.MatrixBuilder;
import de.tuberlin.pserver.math.tuples.Tuple2;
import de.tuberlin.pserver.types.DistributedMatrix;

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
    // Fields.
    // ---------------------------------------------------

    // input. i.e. activation vectors of a neuronal network
    @State(scope = Scope.REPLICATED, rows = ROWS, cols = INPUT_COLS, path = "datasets/mnist_10_X.csv")
    public Matrix32F X;


    // high dimensional affinity function (for two vectors of input space)
    @State(scope = Scope.PARTITIONED, rows = ROWS, cols = ROWS)
    public DistributedMatrix P;

    // model. linear embedding.
    @State(scope = Scope.REPLICATED,
           rows = ROWS, cols = EMBEDDING_DIMENSION,
           path = "datasets/mnist_10_initY.csv")

    public Matrix32F Y;

    /*@StateMerger(stateObjects = "Y")
    public final MatrixUpdateMerger YUpdater = (i, j, val, remoteVal) -> {
        Matrix.PartitionShape shape = P.getPartitionShape();
        if(i >= shape.rowOffset && i < shape.rowOffset + shape.rows) {
            return val;
        }
        else {
            return remoteVal;
        }
    };*/

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Unit
    public void main(final Lifecycle lifecycle) {

        /*program.process(() -> {
            // calc affinity. P is affinity for input X
            P.assign(binarySearch(X, TOL, PERPLEXITY));
            // symmetrize
            Matrix PT = P.transpose();
            P.add(PT, P);
            // keep pdf properties
            double sumP = P.aggregateRows(Matrix::sum).sum();
            P.scale(1 / sumP, P);
            // early exaggeration
            P.scale(EARLY_EXAGGERATION, P);


            final long n = Y.rows();
            final long d = Y.cols();

            // Q is affinity for model Y
            final Matrix Q            = new MatrixBuilder().dimension(n, n).build();
            // for calc of Q
            final Matrix Y_squared    = new MatrixBuilder().dimension(n, d).build();
            // strengthen good direction, weaken bad ones (in gradient descent)
            final Matrix gains        = new MatrixBuilder().dimension(n, d).build();
            // previous gradient. moment of direction "movement"
            final Matrix iY           = new MatrixBuilder().dimension(n, d).build();
            // current gradient
            final Matrix dY           = new MatrixBuilder().dimension(n, d).build();
            // need to center Y in each iteration. define reusable matrix here
            final Matrix mean         = new MatrixBuilder().dimension(1, d).build();

            gains.assign(1.0);

            // annealing factor
            final MutableDouble momentum = new MutableDouble(0.0);

            CF.loop()
                    .exe(NUM_EPOCHS, (epoch) -> {

                        // calc distance matrix of Y
                        Y.applyOnElements(e -> Math.pow(e, 2), Y_squared);
                        final Matrix sum_Y = Y_squared.aggregateRows(Matrix::sum);
                        final Matrix num = Y.mul(Y.transpose())
                                .scale(-2)
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
                        final double sumNum = num.aggregateRows(Matrix::sum).sum();

                        num.scale(1.0 / sumNum, Q);
                        Q.applyOnElements(e -> Math.max(e, 1e-12), Q);

                        //final Matrix PQ = P.sub(Q);

                        Matrix.PartitionShape shape = P.getPartitionShape();
                        CF.loop().exe(shape.rows, (i) -> {
                            // TODO: get target vector of dY instead. possible? or resuable?
                            final Matrix sumVec = new MatrixBuilder().dimension(1, d).build();
                            CF.loop().exe(P.cols(), (j) -> {
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
                        CF.loop()
                                .exe(gains, (e, i, j, v) -> {
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

                        DF.publishUpdate();
                        executionManager.globalSync(programContext);
                        DF.pullUpdate();

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

        }).postProcess(() -> result(Y));*/
    }

    // ---------------------------------------------------
    // Helper Methods.
    // ---------------------------------------------------

    private Matrix32F binarySearch(Matrix32F X, Double tol, Double perplexity) {

        PartitionShape shape = P.getPartitionShape();

        long n = shape.rows;

        final Matrix32F P_tmp      = new MatrixBuilder().dimension(shape.rows, shape.cols).build();
        // beta = 1/(2*sigma^2). sigma is depended on a point. so we have n sigmas
        final Matrix32F beta       = new MatrixBuilder().dimension(1, n).build();

        beta.assign(1f);

        final Matrix32F sumX = X.applyOnElements(e -> (float)Math.pow(e, 2)).aggregateRows(Matrix<Float>::sum);
        // X * X^. Then we have N x N. scale with -2. Add sumX row-wise and col-wise.
        // distance matrix of X
        final Matrix32F D = X.mul(X.transpose()).scale(-2f).addVectorToRows(sumX).transpose().addVectorToRows(sumX);

        final float logU = (float)Math.log(perplexity);

        for (long i=0; i < n; ++i) {

            float betaMin = Float.NEGATIVE_INFINITY;
            float betaMax = Float.POSITIVE_INFINITY;

            final Matrix32F Di = D.getRow(shape.rowOffset + i);
            Tuple2<Float, Matrix32F> hBeta = computeHBeta(Di, beta.get(i), i);

            double H = hBeta._1;
            Matrix32F Pi = hBeta._2;
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

    private Tuple2<Float, Matrix32F> computeHBeta(Matrix32F d, Float beta, Long index){
        final Matrix32F P = new MatrixBuilder().dimension(1, d.cols()).build();
        P.set(0, index, 0f);

        // parExe over all elements i != j
        for (long i=0; i < P.cols(); ++i) {
            if (i != index) {
                P.set(0, i, (float) Math.exp(-1 * d.get(i) * beta));
            }
        }

        final Matrix32F PD = P.applyOnElements(d, (e1, e2) -> e1 * e2);
        final float sumP = P.sum();
        final float H = (float)Math.log(sumP) + beta * PD.sum() / sumP;

        P.applyOnElements(e -> e / sumP, P);

        return new Tuple2<>(H, P);
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void cluster() {
        System.setProperty("pserver.profile", "wally");
        PServerExecutor.DISTRIBUTED
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

            Matrix R = (Matrix) res.get(0).get(0);
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