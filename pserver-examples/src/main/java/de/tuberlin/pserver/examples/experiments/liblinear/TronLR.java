package de.tuberlin.pserver.examples.experiments.liblinear;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.dataflow.aggregators.Aggregator;
import de.tuberlin.pserver.dsl.dataflow.shared.SharedDouble;
import de.tuberlin.pserver.dsl.dataflow.shared.SharedVar;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.runtime.SlotContext;


public class TronLR implements TronFunction {

    private final SlotContext sc;

    public TronLR(final SlotContext sc) { this.sc = Preconditions.checkNotNull(sc); }

    public double functionValue(final Matrix dataPoints, final Vector w_broad, final Parameter param) throws Exception {

        final SharedDouble f_obj = new SharedDouble(sc, 0.0);

        sc.CF.loop().parExe(dataPoints, (epoch, it) -> {

            double z = 0.0;

            for (int i = 0; i < it.cols(); ++i)
                if (it.value(i) != 0.0)
                    z += it.value(i) * w_broad.get(i);

            final double yz = it.value(it.cols() - 1) * z;

            final double enyz = Math.exp(-yz);

            f_obj.add((yz >= 0) ? Math.log(1 + enyz) : -yz + Math.log(1 + Math.exp(yz)));
        });

        return new Aggregator<>(sc, f_obj.done().get())
                .apply(partialAggs -> partialAggs.stream()
                                .map(Double::doubleValue)
                                .reduce((a, b) -> a + b)
                                .get() * param.C + (0.5 * w_broad.dot(w_broad))
                );
    }

    public Vector gradient(final Matrix dataPoints, final Vector w_broad, final Parameter param) throws Exception {

        final Vector grad = new SharedVar<>(sc, w_broad.copy().assign(0.0)).acquire();

        sc.CF.loop().parExe(dataPoints, (epoch, it) -> {

            double z = 0.0;

            for (int i = 0; i < it.cols(); ++i)
                if (it.value(i) != 0.0)
                    z += it.value(i) * w_broad.get(i);

            final double y = it.value(it.cols() - 1);
            z = (1.0 / (1.0 + Math.exp(-y * z)) - 1.0) * y;

            for (int i = 0; i < it.cols(); ++i)
                if (it.value(i) != 0.0) {
                    synchronized (grad) {
                        grad.set(i, grad.get(i) + z * it.value(i));
                    }
                }
        });

        return new Aggregator<>(sc, grad)
                .apply(partialGrads -> partialGrads.stream()
                                .reduce((a, b) -> a.add(b, a))
                                .get().mul(param.C).add(grad)
                );
    }

    public Vector hessianVector(final Matrix dataPoints, final Vector w_broad, final Parameter param, final Vector s) throws Exception {

        final Vector blockHs = new SharedVar<>(sc, w_broad.copy().assign(0.0)).acquire();

        sc.CF.loop().parExe(dataPoints, (epoch, it) -> {

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
                        blockHs.set(i, blockHs.get(i) + wa * it.value(i));
                    }
                }
            }
        });

        return new Aggregator<>(sc, blockHs)
                .apply(partialGrads -> partialGrads.stream()
                                .reduce((a, b) -> a.add(b, a))
                                .get().mul(param.C).add(s)
                );
    }
}
