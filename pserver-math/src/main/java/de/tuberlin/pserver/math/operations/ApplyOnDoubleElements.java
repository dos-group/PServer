package de.tuberlin.pserver.math.operations;

import de.tuberlin.pserver.math.exceptions.IncompatibleShapeException;

import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

public interface ApplyOnDoubleElements<T> {

    /**
     * Identical to {@link #applyOnElements(DoubleUnaryOperator, T)} but automatically creates the resulting B.
     */
    T applyOnElements(final DoubleUnaryOperator f);

    /**
     * Called on A. Computes B = f(A) element-wise. <br>
     * <strong>Note: A and B have to be of the same shape</strong>
     *
     * @param f Unary higher order function f: x -> y
     * @param B to store the result in
     * @return B after computing B = f(A) element-wise.
     * @throws IncompatibleShapeException If the shapes of B and A are not equal
     */
    T applyOnElements(final DoubleUnaryOperator f, final T B);

    /**
     * Identical to {@link #applyOnElements(T, DoubleBinaryOperator, T)} but automatically creates the resulting C.
     */
    T applyOnElements(final T B, final DoubleBinaryOperator f);

    /**
     * Called on A. Computes C = f(A, B) element-wise.<br>
     * <strong>Note: A and B are wlog. of shape n x m and o x p respectively. It has to hold that n <= o and m <= p. Also the shape of A an C have to be the same.</strong
     *
     * @param B containing the elements that are used as second arguments in f
     * @param f Binary higher order function f: x, y -> z
     * @param C to store the result in
     * @return A after computing  C = f(A, B) element-wise.
     * @throws IncompatibleShapeException If the shape of B is smaller than the one of A in any dimension or if the shapes of C and A are not equal
     */
    T applyOnElements(final T B, final DoubleBinaryOperator f, final T C);

}
