package de.tuberlin.pserver.dsl.controlflow.program;

import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.runtime.MLProgram;
import org.apache.commons.lang3.mutable.MutableDouble;

public class Test extends MLProgram {

    // ---------------------------------------------------

    private static final int        ROWS = 50;

    private static final int        COLS = 36073 * 2;

    private static final double     ALPHA = 0.75;

    private static final int        X_MAX = 10;

    private static final double     LEARNING_RATE = 0.05;

    // ---------------------------------------------------

    @State(scope = State.SHARED, rows = 15000, cols = 200, path = "datasets/test.csv", format = Format.SPARSE_FORMAT)
    private Matrix X;

    @State(scope = State.GLOBAL | State.SHARED, rows = ROWS, cols = COLS)
    private Matrix W;

    @State(scope = State.GLOBAL | State.SHARED, rows = ROWS, cols = COLS)
    private Matrix GradSq;

    @State(scope = State.GLOBAL | State.SHARED, rows = COLS)
    private Vector B;

    @State(scope = State.GLOBAL | State.SHARED, rows = COLS)
    private Vector GradSqB;

    // ---------------------------------------------------

    public void define(final Program program) {

        program.process(() -> {

            int offset = (COLS / slotContext.programContext.runtimeContext.numOfNodes * slotContext.programContext.runtimeContext.nodeID)
                    + (COLS / slotContext.programContext.runtimeContext.numOfNodes / slotContext.programContext.perNodeDOP)
                    * slotContext.slotID;

            CF.iterate().exe(15, (e0) -> {

                final MutableDouble costI = new MutableDouble(0.0);

                CF.iterate().parExe(X, (e1, i, j, v) -> {

                    long wordVecIdx = offset + i;
                    long ctxVecIdx = j + COLS;

                    Double xVal = X.get(i, j);

                    if (xVal == 0)
                        return;

                    Vector w1 = W.colAsVector(wordVecIdx);
                    Double b1 = B.get(wordVecIdx);
                    Vector gs1 = GradSq.colAsVector(wordVecIdx);

                    Vector w2 = W.colAsVector(ctxVecIdx);
                    Double b2 = B.get(ctxVecIdx);
                    Vector gs2 = GradSq.colAsVector(ctxVecIdx);

                    double diff = w1.dot(w2) + b1 + b2 - Math.log(xVal);
                    double fdiff = (xVal > X_MAX) ? diff : Math.pow(xVal / X_MAX, ALPHA) * diff;

                    costI.add(0.5 * diff * fdiff);

                    fdiff *= LEARNING_RATE;

                    Vector grad1 = w2.mul(fdiff);
                    Vector grad2 = w1.mul(fdiff);

                    W.assignColumn(wordVecIdx, w1.add(-1, grad1.applyOnElements(gs1, (el1, el2) -> el1 / Math.sqrt(el2))));
                    W.assignColumn(ctxVecIdx, w2.add(-1, grad2.applyOnElements(gs2, (el1, el2) -> el1 / Math.sqrt(el2))));

                    B.set(wordVecIdx, b1 - fdiff / Math.sqrt(GradSqB.get(wordVecIdx)));
                    B.set(ctxVecIdx, b2 - fdiff / Math.sqrt(GradSqB.get(ctxVecIdx)));

                    gs1 = gs1.applyOnElements(grad1, (el1, el2) -> el1 + el2 * el2);
                    gs2 = gs2.applyOnElements(grad2, (el1, el2) -> el1 + el2 * el2);

                    GradSq.assignColumn(wordVecIdx, gs1);
                    GradSq.assignColumn(ctxVecIdx, gs2);

                    GradSqB.set(wordVecIdx, GradSqB.get(wordVecIdx) + fdiff * fdiff);
                    GradSqB.set(ctxVecIdx, GradSqB.get(ctxVecIdx) + fdiff * fdiff);
                });

            });

        });
    }
}
