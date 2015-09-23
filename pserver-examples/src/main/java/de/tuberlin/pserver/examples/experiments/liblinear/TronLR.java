package de.tuberlin.pserver.examples.experiments.liblinear;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.dataflow.aggregators.Aggregator;
import de.tuberlin.pserver.dsl.dataflow.shared.SharedDouble;
import de.tuberlin.pserver.dsl.dataflow.shared.SharedVar;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.ProgramContext;


public class TronLR implements TronFunction {

    private final ProgramContext sc;

    public TronLR(final ProgramContext sc) { this.sc = Preconditions.checkNotNull(sc); }

    public double functionValue(final Matrix dataPoints, final Matrix w_broad, final Parameter param) throws Exception {

        final SharedDouble f_obj = new SharedDouble(sc, 0.0);

        /*sc.CF.loop().parExe(dataPoints, (epoch, it) -> {

            double z = 0.0;

            for (int i = 0; i < it.cols(); ++i)
                if (it.value(i) != 0.0)
                    z += it.value(i) * w_broad.get(0, i);

            final double yz = it.value(it.cols() - 1) * z;

            final double enyz = Math.exp(-yz);

            f_obj.add((yz >= 0) ? Math.log(1 + enyz) : -yz + Math.log(1 + Math.exp(yz)));
        });*/

        return new Aggregator<>(sc, f_obj.done().get())
                .apply(partialAggs -> partialAggs.stream()
                                .map(Double::doubleValue)
                                .reduce((a, b) -> a + b)
                                .get() * param.C + (0.5 * w_broad.dot(w_broad))
                );
    }

    public Matrix gradient(final Matrix dataPoints, final Matrix w_broad, final Parameter param) throws Exception {

        final Matrix grad = new SharedVar<>(sc, w_broad.copy().assign(0.0)).acquire();

        /*sc.CF.loop().parExe(dataPoints, (epoch, it) -> {

            double z = 0.0;

            for (int i = 0; i < it.cols(); ++i)
                if (it.value(i) != 0.0)
                    z += it.value(i) * w_broad.get(0, i);

            final double y = it.value(it.cols() - 1);
            z = (1.0 / (1.0 + Math.exp(-y * z)) - 1.0) * y;

            for (int i = 0; i < it.cols(); ++i)
                if (it.value(i) != 0.0) {
                    synchronized (grad) {
                        grad.set(0, i, grad.get(0, i) + z * it.value(i));
                    }
                }
        });*/

        return new Aggregator<>(sc, grad)
                .apply(partialGrads -> partialGrads.stream()
                                .reduce((a, b) -> a.add(b, a))
                                .get().scale(param.C).add(grad)
                );
    }

    public Matrix hessianVector(final Matrix dataPoints, final Matrix w_broad, final Parameter param, final Matrix s) throws Exception {

        final Matrix blockHs = new SharedVar<>(sc, w_broad.copy().assign(0.0)).acquire();

        /*sc.CF.loop().parExe(dataPoints, (epoch, it) -> {

            double z = 0.0;
            double wa = 0.0;

            for (int i = 0; i < it.cols(); ++i) {
                if (it.value(i) != 0.0) {
                    z += it.value(i) * w_broad.get(i);
                    wa += it.value(i) * s.get(i);
                }
            }

            final double y = it.value(it.cols() - 1);
            final double sigma = 1.0 / (1.0 + Math.exp(-y * z));
            final double D = sigma * (1.0 - sigma);
            wa = D * wa;

            for (int i = 0; i < it.cols(); ++i) {
                if (it.value(i) != 0.0) {
                    synchronized (blockHs) {
                        blockHs.set(0, i, blockHs.get(i) + wa * it.value(i));
                    }
                }
            }
        });*/

        return new Aggregator<>(sc, blockHs)
                .apply(partialGrads -> partialGrads.stream()
                                .reduce((a, b) -> a.add(b, a))
                                .get().scale(param.C).add(s)
                );
    }
}
