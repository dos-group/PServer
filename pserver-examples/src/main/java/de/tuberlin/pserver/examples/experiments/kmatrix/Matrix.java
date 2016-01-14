package de.tuberlin.pserver.examples.experiments.kmatrix;


import java.math.BigInteger;
import java.util.Vector;

public class Matrix {
    private int _n, _k;
    private int[][] _m;
    private static int[][] _buffer;

    public Matrix(int n, int k) {
        _n = n;
        _k = k;
        _m = new int[_n][_n];
        if (_buffer == null) {
            _buffer = new int[_n][_n];
        }
    }

    public Matrix(int n, int k, BigInteger id) {
        _n = n;
        _k = k;
        _m = new int[_n][_n];
        if (_buffer == null) {
            _buffer = new int[_n][_n];
        }
        BigInteger K = BigInteger.valueOf(_k);
        for (int i = 0; i < _n * _n; ++i) {
            BigInteger rest = id.mod(K);
            _m[i / _n][i % _n] = rest.intValue();
            id = id.divide(K);
        }
    }

    public Matrix(Matrix o) {
        _n = o._n;
        _k = o._k;
        _m = new int[_n][_n];
        for (int r = 0; r < _n; ++r) {
            for (int c = 0; c < _n; ++c) {
                _m[r][c] = o._m[r][c];
            }
        }
    }

    public boolean is_identity() {
        for (int r = 0; r < _n; ++r) {
            for (int c = 0; c < _n; ++c) {
                if ((r == c && _m[r][c] != 1) || (r != c && _m[r][c] != 0)) {
                    return false;
                }
            }
        }

        return true;
    }

    public void mult(Matrix o) {
        for (int r = 0; r < _n; ++r) {
            for (int c = 0; c < _n; ++c) {
                int sum = 0;
                for (int i = 0; i < _n; ++i) {
                    sum += _m[r][i] * o._m[i][c];
                }
                _buffer[r][c] = sum % _k;
            }
        }
        int[][] tmp = _buffer;
        _buffer = _m;
        _m = tmp;
    }

    public BigInteger get_id() {
        BigInteger id = BigInteger.valueOf(0);
        BigInteger K = BigInteger.valueOf(_k);
        for (int i = 0; i < _n * _n; ++i) {
            int j = _n * _n - 1 - i;
            id = id.multiply(K);
            id = id.add(BigInteger.valueOf(_m[j / _n][j % _n]));
        }

        return id;
    }

    public static class NonsingularIterator {
        private int _n, _k;
        private int _count;
        private boolean _done;
        Matrix m;
        Vector<Integer> coefficients;
        Vector<Integer> base_rows;
        Vector<Vector<Integer>> all_rows;
        boolean[] acceptable_rows;
        Vector<Vector<Integer>> ruled_out_rows;
        Vector<Integer> base_ruled_out_rows;
        int[] state;
        boolean include_permutations = true;

        public NonsingularIterator(int n, int k) {
            _n = n;
            _k = k;

            m = new Matrix(_n, _k);
            Vector<Integer> row = new Vector<Integer>(_n);
            ruled_out_rows = new Vector<Vector<Integer>>(_n);
            state = new int[_n];
            int max_rows = 1;
            for (int i = 0; i < _n; ++i) {
                max_rows *= _k;
                row.add(0);
                ruled_out_rows.add(new Vector<Integer>(max_rows));
            }

            coefficients = new Vector<Integer>();
            for (int i = 1; i < _k; ++i) {
                if (Util.gcd(i, _k) == 1) {
                    coefficients.add(i);
                }
            }

            all_rows = new Vector<Vector<Integer>>();
            base_rows = new Vector<Integer>();
            base_ruled_out_rows = new Vector<Integer>();
            all_rows.add(new Vector<Integer>(row));
            while (true) {
                int i = 0;
                boolean found_one = false;
                while (i < _n) {
                    row.set(i, (row.get(i) + 1) % _k);
                    if (row.get(i) != 0) {
                        found_one = true;
                        break;
                    }
                    ++i;
                }
                if (!found_one) {
                    break;
                }
                all_rows.add(new Vector<Integer>(row));
            }

            acceptable_rows = new boolean[all_rows.size()];
            for (int i = 0; i < all_rows.size(); ++i) {
                int g = _k;
                for (int j = 0; j < row.size(); ++j) {
                    g = Util.gcd(g, all_rows.get(i).get(j));
                    if (g == 1) {
                        break;
                    }
                }
                if (g == 1) {
                    base_rows.add(i);
                    acceptable_rows[i] = true;
                } else {
                    base_ruled_out_rows.add(i);
                }
            }

            // printf("rows: %d, usable: %d\n", (int) all_rows.size(),
            //    (int) base_rows.size());
            for (int i = 0; i < _n; ++i) {
                state[i] = -1;
            }
            _done = false;
            set_count(0);
        }

        public void set_count(int count) {
            _count = count;
        }

        public boolean is_done() {
            return _done;
        }

        public int get_count() {
            return _count;
        }

        public void generate_next() {
            if (!is_done()) {
                next_matrix(_n - 1);
                ++_count;
            }
        }

        public Matrix matrix() {
            return m;
        }

        boolean restore(int count, BigInteger id) {
            _done = false;
            set_count(count);
            m = new Matrix(_n, _k, id);

            for (int r = 0; r < _n; ++r) {
                clear_ruled_out_rows(r);
            }

            for (int r = 0; r < _n; ++r) {
                int tmp = 0;
                for (int j = 0; j < _n; ++j) {
                    tmp *= _k;
                    tmp += m._m[r][_n - 1 - j];
                }

                if (!acceptable_rows[tmp]) {
                    return false;
                }
                for (int i = 0; i < base_rows.size(); ++i) {
                    if (base_rows.get(i) == tmp) {
                        state[r] = i;
                        break;
                    }
                }
                if (r < _n - 1) {
                    rule_out_rows(r);
                }
            }
            return true;
        }

        void next_matrix(int row_number) {
            while (true) {
                if (_done) {
                    return;
                }
                if (row_number > 0 && state[row_number - 1] == -1) {
                    next_matrix(row_number - 1);
                }

                clear_ruled_out_rows(row_number);

                if (!include_permutations && state[row_number] == -1 && row_number > 0) {
                    state[row_number] = state[row_number - 1];
                    // printf("new: %d:%d %d:%d\n", row_number, state[row_number],
                    // row_number - 1, state[row_number - 1]);
                }
                for (int i = state[row_number] + 1; i < base_rows.size(); ++i) {
                    int id = base_rows.get(i);
                    if (!acceptable_rows[id]) {
                        continue;
                    }
                    Vector<Integer> row = all_rows.get(id);

                    state[row_number] = i;
                    for (int j = 0; j < _n; ++j) {
                        m._m[row_number][j] = row.get(j);
                    }
                    if (row_number < _n - 1) {
                        rule_out_rows(row_number);
                    }
                    return;
                }

                if (row_number == 0) {
                    _done = true;
                } else {
                    state[row_number] = -1;
                    next_matrix(row_number - 1);
                }
            }
        }

        void clear_ruled_out_rows(int row_number) {
            Vector<Integer> relevant = ruled_out_rows.get(row_number);
            for (int i = 0; i < relevant.size(); ++i) {
                acceptable_rows[relevant.get(i)] = true;
            }
            relevant.clear();
        }

        void rule_out_rows(int row_number) {
            // this contains the 0 row and for composite _k all non relative prime multiples of
            // rows
            for (int j = 0; j < base_ruled_out_rows.size(); ++j) {
                Vector<Integer> row = all_rows.get(base_ruled_out_rows.get(j));
                for (int c = 0; c < coefficients.size(); ++c) {
                    int tmp = 0;
                    for (int ii = 0; ii < _n; ++ii) {
                        tmp *= _k;
                        tmp += (row.get(_n - 1 - ii) +
                                all_rows.get(base_rows.get(state[row_number])).get(_n - 1 - ii)
                                        * coefficients.get(c)) % _k;
                    }
                    if (acceptable_rows[tmp]) {
                        acceptable_rows[tmp] = false;
                        ruled_out_rows.get(row_number).add(tmp);
                    }
                }
            }

            for (int i = 0; i < row_number; ++i) {
                for (int j = 0; j < ruled_out_rows.get(i).size(); ++j) {
                    Vector<Integer> row = all_rows.get(ruled_out_rows.get(i).get(j));
                    for (int c = 0; c < coefficients.size(); ++c) {
                        int tmp = 0;
                        for (int ii = 0; ii < _n; ++ii) {
                            tmp *= _k;
                            tmp += (row.get(_n - 1 - ii) +
                                    all_rows.get(base_rows.get(state[row_number])).get(_n - 1 - ii)
                                            * coefficients.get(c)) % _k;
                        }
                        if (acceptable_rows[tmp]) {
                            acceptable_rows[tmp] = false;
                            ruled_out_rows.get(row_number).add(tmp);
                        }
                    }
                }
            }
        }
    }
}
