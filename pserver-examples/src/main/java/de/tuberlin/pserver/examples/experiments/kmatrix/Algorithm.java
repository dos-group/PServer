package de.tuberlin.pserver.examples.experiments.kmatrix;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Vector;

public class Algorithm {
    static private HashSet<BigInteger> _seen = new HashSet<BigInteger>();

    private static Vector<BigInteger> get_cycle(Matrix base) {
        Vector<BigInteger> cycle = new Vector<BigInteger>();
        Matrix tmp = new Matrix(base);

        cycle.add(tmp.get_id());
        while (!tmp.is_identity()) {
            tmp.mult(base);
            cycle.add(tmp.get_id());
        }

        return cycle;
    }

    public static Vector<Vector<BigInteger>> get_cycles(int n, int k, BigInteger start_id, int chunk_size) {
        Vector<Vector<BigInteger>> cycles = new Vector<Vector<BigInteger>>();
        Matrix.NonsingularIterator it = new Matrix.NonsingularIterator(n, k);
        it.restore(0, start_id);

        if (it.is_done()) {
            return cycles;
        }

        for (; !it.is_done() && it.get_count() < chunk_size; it.generate_next()) {
            Matrix m = it.matrix();
            // check whether m.get_id() was already seen somewhere
            if (_seen.contains(m.get_id())) {
                continue;
            }
            Vector<BigInteger> cycle = get_cycle(m);
            System.out.format("%d", cycle.size());
            for (BigInteger id : cycle) {
                _seen.add(id);
                System.out.format(" %s", id.toString());
            }
            System.out.println();

            cycles.add(cycle);
        }

        return cycles;
    }
}
