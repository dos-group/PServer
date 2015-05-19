package de.tuberlin.pserver.math;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
Copyright 1999 CERN - European Organization for Nuclear Research.
Permission to use, copy, modify, distribute and sell this software and its documentation for any purpose
is hereby granted without fee, provided that the above copyright notice appear in all copies and
that both that copyright notice and this permission notice appear in supporting documentation.
CERN makes no representations about the suitability of this software for any purpose.
It is provided "as is" without expressed or implied warranty.
*/

public final class Functions {

  /*
   * <H3>Unary functions</H3>
   */
    /** Function that returns <tt>Math.abs(a)</tt>. */
    public static final DoubleFunction ABS = new DoubleFunction() {
        @Override
        public double apply(double a) {
            return Math.abs(a);
        }
    };

    /** Function that returns <tt>Math.acos(a)</tt>. */
    public static final DoubleFunction ACOS = new DoubleFunction() {
        @Override
        public double apply(double a) {
            return Math.acos(a);
        }
    };

    /** Function that returns <tt>Math.asin(a)</tt>. */
    public static final DoubleFunction ASIN = new DoubleFunction() {
        @Override
        public double apply(double a) {
            return Math.asin(a);
        }
    };

    /** Function that returns <tt>Math.atan(a)</tt>. */
    public static final DoubleFunction ATAN = new DoubleFunction() {
        @Override
        public double apply(double a) {
            return Math.atan(a);
        }
    };

    /** Function that returns <tt>Math.ceil(a)</tt>. */
    public static final DoubleFunction CEIL = new DoubleFunction() {

        @Override
        public double apply(double a) {
            return Math.ceil(a);
        }
    };

    /** Function that returns <tt>Math.cos(a)</tt>. */
    public static final DoubleFunction COS = new DoubleFunction() {

        @Override
        public double apply(double a) {
            return Math.cos(a);
        }
    };

    /** Function that returns <tt>Math.exp(a)</tt>. */
    public static final DoubleFunction EXP = new DoubleFunction() {

        @Override
        public double apply(double a) {
            return Math.exp(a);
        }
    };

    /** Function that returns <tt>Math.floor(a)</tt>. */
    public static final DoubleFunction FLOOR = new DoubleFunction() {

        @Override
        public double apply(double a) {
            return Math.floor(a);
        }
    };

    /** Function that returns its argument. */
    public static final DoubleFunction IDENTITY = new DoubleFunction() {

        @Override
        public double apply(double a) {
            return a;
        }
    };

    /** Function that returns <tt>1.0 / a</tt>. */
    public static final DoubleFunction INV = new DoubleFunction() {

        @Override
        public double apply(double a) {
            return 1.0 / a;
        }
    };

    /** Function that returns <tt>Math.log(a)</tt>. */
    public static final DoubleFunction LOGARITHM = new DoubleFunction() {

        @Override
        public double apply(double a) {
            return Math.log(a);
        }
    };

    /** Function that returns <tt>Math.log(a) / Math.log(2)</tt>. */
    public static final DoubleFunction LOG2 = new DoubleFunction() {

        @Override
        public double apply(double a) {
            return Math.log(a) * 1.4426950408889634;
        }
    };

    /** Function that returns <tt>-a</tt>. */
    public static final DoubleFunction NEGATE = new DoubleFunction() {

        @Override
        public double apply(double a) {
            return -a;
        }
    };

    /** Function that returns <tt>Math.rint(a)</tt>. */
    public static final DoubleFunction RINT = new DoubleFunction() {

        @Override
        public double apply(double a) {
            return Math.rint(a);
        }
    };

    /** Function that returns <tt>a < 0 ? -1 : a > 0 ? 1 : 0</tt>. */
    public static final DoubleFunction SIGN = new DoubleFunction() {

        @Override
        public double apply(double a) {
            return a < 0 ? -1 : a > 0 ? 1 : 0;
        }
    };

    /** Function that returns <tt>Math.sin(a)</tt>. */
    public static final DoubleFunction SIN = new DoubleFunction() {

        @Override
        public double apply(double a) {
            return Math.sin(a);
        }
    };

    /** Function that returns <tt>Math.sqrt(a)</tt>. */
    public static final DoubleFunction SQRT = new DoubleFunction() {

        @Override
        public double apply(double a) {
            return Math.sqrt(a);
        }
    };

    /** Function that returns <tt>a * a</tt>. */
    public static final DoubleFunction SQUARE = new DoubleFunction() {

        @Override
        public double apply(double a) {
            return a * a;
        }
    };

    /** Function that returns <tt> 1 / (1 + exp(-a) </tt> */
    public static final DoubleFunction SIGMOID = new DoubleFunction() {
        @Override
        public double apply(double a) {
            return 1.0 / (1.0 + Math.exp(-a));
        }
    };

    /** Function that returns <tt> a * (1-a) </tt> */
    public static final DoubleFunction SIGMOIDGRADIENT = new DoubleFunction() {
        @Override
        public double apply(double a) {
            return a * (1.0 - a);
        }
    };

    /** Function that returns <tt>Math.tan(a)</tt>. */
    public static final DoubleFunction TAN = new DoubleFunction() {

        @Override
        public double apply(double a) {
            return Math.tan(a);
        }
    };


  /*
   * <H3>Binary functions</H3>
   */

    /** Function that returns <tt>Math.atan2(a,b)</tt>. */
    public static final DoubleDoubleFunction ATAN2 = new DoubleDoubleFunction() {

        @Override
        public double apply(double a, double b) {
            return Math.atan2(a, b);
        }
    };

    /** Function that returns <tt>a < b ? -1 : a > b ? 1 : 0</tt>. */
    public static final DoubleDoubleFunction COMPARE = new DoubleDoubleFunction() {

        @Override
        public double apply(double a, double b) {
            return a < b ? -1 : a > b ? 1 : 0;
        }
    };

    /** Function that returns <tt>a / b</tt>. */
    public static final DoubleDoubleFunction DIV = new DoubleDoubleFunction() {

        @Override
        public double apply(double a, double b) {
            return a / b;
        }
    };

    /** Function that returns <tt>a == b ? 1 : 0</tt>. */
    public static final DoubleDoubleFunction EQUALS = new DoubleDoubleFunction() {

        @Override
        public double apply(double a, double b) {
            return a == b ? 1 : 0;
        }
    };

    /** Function that returns <tt>a > b ? 1 : 0</tt>. */
    public static final DoubleDoubleFunction GREATER = new DoubleDoubleFunction() {

        @Override
        public double apply(double a, double b) {
            return a > b ? 1 : 0;
        }
    };

    /** Function that returns <tt>Math.IEEEremainder(a,b)</tt>. */
    public static final DoubleDoubleFunction IEEE_REMAINDER = new DoubleDoubleFunction() {

        @Override
        public double apply(double a, double b) {
            return Math.IEEEremainder(a, b);
        }
    };

    /** Function that returns <tt>a == b</tt>. */
    /*public static final DoubleDoubleProcedure IS_EQUAL = new DoubleDoubleProcedure() {

        @Override
        public boolean apply(double a, double b) {
            return a == b;
        }
    };*/

    /** Function that returns <tt>a < b</tt>. */
    /*public static final DoubleDoubleProcedure IS_LESS = new DoubleDoubleProcedure() {

        @Override
        public boolean apply(double a, double b) {
            return a < b;
        }
    };*/

    /** Function that returns <tt>a > b</tt>. */
    /*public static final DoubleDoubleProcedure IS_GREATER = new DoubleDoubleProcedure() {

        @Override
        public boolean apply(double a, double b) {
            return a > b;
        }
    };*/

    /** Function that returns <tt>a < b ? 1 : 0</tt>. */
    public static final DoubleDoubleFunction LESS = new DoubleDoubleFunction() {

        @Override
        public double apply(double a, double b) {
            return a < b ? 1 : 0;
        }
    };

    /** Function that returns <tt>Math.log(a) / Math.log(b)</tt>. */
    public static final DoubleDoubleFunction LG = new DoubleDoubleFunction() {

        @Override
        public double apply(double a, double b) {
            return Math.log(a) / Math.log(b);
        }
    };

    /** Function that returns <tt>Math.max(a,b)</tt>. */
    public static final DoubleDoubleFunction MAX = new DoubleDoubleFunction() {

        @Override
        public double apply(double a, double b) {
            return Math.max(a, b);
        }
    };

    /** Function that returns <tt>Math.min(a,b)</tt>. */
    public static final DoubleDoubleFunction MIN = new DoubleDoubleFunction() {

        @Override
        public double apply(double a, double b) {
            return Math.min(a, b);
        }
    };

    /** Function that returns <tt>a - b</tt>. */
    public static final DoubleDoubleFunction MINUS = plusMult(-1);
  /*
  new DoubleDoubleFunction() {
    public final double apply(double a, double b) { return a - b; }
  };
  */

    /** Function that returns <tt>a % b</tt>. */
    public static final DoubleDoubleFunction MOD = new DoubleDoubleFunction() {

        @Override
        public double apply(double a, double b) {
            return a % b;
        }
    };

    /** Function that returns <tt>a * b</tt>. */
    public static final DoubleDoubleFunction MULT = new DoubleDoubleFunction() {

        @Override
        public double apply(double a, double b) {
            return a * b;
        }
    };

    /** Function that returns <tt>a + b</tt>. */
    public static final DoubleDoubleFunction PLUS = new DoubleDoubleFunction() {

        @Override
        public double apply(double a, double b) {
            return a + b;
        }
    };

    /** Function that returns <tt>Math.abs(a) + Math.abs(b)</tt>. */
    public static final DoubleDoubleFunction PLUS_ABS = new DoubleDoubleFunction() {

        @Override
        public double apply(double a, double b) {
            return Math.abs(a) + Math.abs(b);
        }
    };

    /** Function that returns <tt>Math.pow(a,b)</tt>. */
    public static final DoubleDoubleFunction POW = new DoubleDoubleFunction() {

        @Override
        public double apply(double a, double b) {
            return Math.pow(a, b);
        }
    };

    private Functions() {
    }

    /**
     * Constructs a function that returns <tt>(from<=a && a<=to) ? 1 : 0</tt>. <tt>a</tt> is a variable, <tt>from</tt> and
     * <tt>to</tt> are fixed.
     */
    public static DoubleFunction between(final double from, final double to) {
        return new DoubleFunction() {

            @Override
            public double apply(double a) {
                return from <= a && a <= to ? 1 : 0;
            }
        };
    }

    /**
     * Constructs a unary function from a binary function with the first operand (argument) fixed to the given constant
     * <tt>c</tt>. The second operand is variable (free).
     *
     * @param function a binary function taking operands in the form <tt>function.apply(c,var)</tt>.
     * @return the unary function <tt>function(c,var)</tt>.
     */
    public static DoubleFunction bindArg1(final DoubleDoubleFunction function, final double c) {
        return new DoubleFunction() {

            @Override
            public double apply(double var) {
                return function.apply(c, var);
            }
        };
    }

    /**
     * Constructs a unary function from a binary function with the second operand (argument) fixed to the given constant
     * <tt>c</tt>. The first operand is variable (free).
     *
     * @param function a binary function taking operands in the form <tt>function.apply(var,c)</tt>.
     * @return the unary function <tt>function(var,c)</tt>.
     */
    public static DoubleFunction bindArg2(final DoubleDoubleFunction function, final double c) {
        return new DoubleFunction() {

            @Override
            public double apply(double var) {
                return function.apply(var, c);
            }
        };
    }

    /**
     * Constructs the function <tt>f( g(a), h(b) )</tt>.
     *
     * @param f a binary function.
     * @param g a unary function.
     * @param h a unary function.
     * @return the binary function <tt>f( g(a), h(b) )</tt>.
     */
    public static DoubleDoubleFunction chain(final DoubleDoubleFunction f, final DoubleFunction g,
                                             final DoubleFunction h) {
        return new DoubleDoubleFunction() {

            @Override
            public double apply(double a, double b) {
                return f.apply(g.apply(a), h.apply(b));
            }
        };
    }

    /**
     * Constructs the function <tt>g( h(a,b) )</tt>.
     *
     * @param g a unary function.
     * @param h a binary function.
     * @return the unary function <tt>g( h(a,b) )</tt>.
     */
    public static DoubleDoubleFunction chain(final DoubleFunction g, final DoubleDoubleFunction h) {
        return new DoubleDoubleFunction() {

            @Override
            public double apply(double a, double b) {
                return g.apply(h.apply(a, b));
            }
        };
    }

    /**
     * Constructs the function <tt>g( h(a) )</tt>.
     *
     * @param g a unary function.
     * @param h a unary function.
     * @return the unary function <tt>g( h(a) )</tt>.
     */
    public static DoubleFunction chain(final DoubleFunction g, final DoubleFunction h) {
        return new DoubleFunction() {

            @Override
            public double apply(double a) {
                return g.apply(h.apply(a));
            }
        };
    }

    /**
     * Constructs a function that returns <tt>a < b ? -1 : a > b ? 1 : 0</tt>. <tt>a</tt> is a variable, <tt>b</tt> is
     * fixed.
     */
    public static DoubleFunction compare(final double b) {
        return new DoubleFunction() {

            @Override
            public double apply(double a) {
                return a < b ? -1 : a > b ? 1 : 0;
            }
        };
    }

    /** Constructs a function that returns the constant <tt>c</tt>. */
    public static DoubleFunction constant(final double c) {
        return new DoubleFunction() {

            @Override
            public double apply(double a) {
                return c;
            }
        };
    }


    /** Constructs a function that returns <tt>a / b</tt>. <tt>a</tt> is a variable, <tt>b</tt> is fixed. */
    //public static DoubleFunction div(double b) {
    //    return mult(1 / b);
    //}

    /** Constructs a function that returns <tt>a == b ? 1 : 0</tt>. <tt>a</tt> is a variable, <tt>b</tt> is fixed. */
    public static DoubleFunction equals(final double b) {
        return new DoubleFunction() {

            @Override
            public double apply(double a) {
                return a == b ? 1 : 0;
            }
        };
    }

    /** Constructs a function that returns <tt>a > b ? 1 : 0</tt>. <tt>a</tt> is a variable, <tt>b</tt> is fixed. */
    public static DoubleFunction greater(final double b) {
        return new DoubleFunction() {

            @Override
            public double apply(double a) {
                return a > b ? 1 : 0;
            }
        };
    }

    /**
     * Constructs a function that returns <tt>Math.IEEEremainder(a,b)</tt>. <tt>a</tt> is a variable, <tt>b</tt> is
     * fixed.
     */
    public static DoubleFunction mathIEEEremainder(final double b) {
        return new DoubleFunction() {

            @Override
            public double apply(double a) {
                return Math.IEEEremainder(a, b);
            }
        };
    }

    /**
     * Constructs a function that returns <tt>from<=a && a<=to</tt>. <tt>a</tt> is a variable, <tt>from</tt> and
     * <tt>to</tt> are fixed.
     */
    /*public static DoubleProcedure isBetween(final double from, final double to) {
        return new DoubleProcedure() {

            @Override
            public boolean apply(double a) {
                return from <= a && a <= to;
            }
        };
    }*/

    /** Constructs a function that returns <tt>a == b</tt>. <tt>a</tt> is a variable, <tt>b</tt> is fixed. */
    /*public static DoubleProcedure isEqual(final double b) {
        return new DoubleProcedure() {

            @Override
            public boolean apply(double a) {
                return a == b;
            }
        };
    }*/

    /** Constructs a function that returns <tt>a > b</tt>. <tt>a</tt> is a variable, <tt>b</tt> is fixed. */
    /*public static DoubleProcedure isGreater(final double b) {
        return new DoubleProcedure() {

            @Override
            public boolean apply(double a) {
                return a > b;
            }
        };
    }*/

    /** Constructs a function that returns <tt>a < b</tt>. <tt>a</tt> is a variable, <tt>b</tt> is fixed. */
    /*public static DoubleProcedure isLess(final double b) {
        return new DoubleProcedure() {

            @Override
            public boolean apply(double a) {
                return a < b;
            }
        };
    }*/

    /** Constructs a function that returns <tt>a < b ? 1 : 0</tt>. <tt>a</tt> is a variable, <tt>b</tt> is fixed. */
    public static DoubleFunction less(final double b) {
        return new DoubleFunction() {

            @Override
            public double apply(double a) {
                return a < b ? 1 : 0;
            }
        };
    }

    /**
     * Constructs a function that returns <tt><tt>Math.log(a) / Math.log(b)</tt></tt>. <tt>a</tt> is a variable,
     * <tt>b</tt> is fixed.
     */
    public static DoubleFunction lg(final double b) {
        return new DoubleFunction() {
            private final double logInv = 1 / Math.log(b); // cached for speed


            @Override
            public double apply(double a) {
                return Math.log(a) * logInv;
            }
        };
    }

    /** Constructs a function that returns <tt>Math.max(a,b)</tt>. <tt>a</tt> is a variable, <tt>b</tt> is fixed. */
    public static DoubleFunction max(final double b) {
        return new DoubleFunction() {

            @Override
            public double apply(double a) {
                return Math.max(a, b);
            }
        };
    }

    /** Constructs a function that returns <tt>Math.min(a,b)</tt>. <tt>a</tt> is a variable, <tt>b</tt> is fixed. */
    public static DoubleFunction min(final double b) {
        return new DoubleFunction() {

            @Override
            public double apply(double a) {
                return Math.min(a, b);
            }
        };
    }

    /** Constructs a function that returns <tt>a - b</tt>. <tt>a</tt> is a variable, <tt>b</tt> is fixed. */
    public static DoubleFunction minus(double b) {
        return plus(-b);
    }

    /**
     * Constructs a function that returns <tt>a - b*constant</tt>. <tt>a</tt> and <tt>b</tt> are variables,
     * <tt>constant</tt> is fixed.
     */
    public static DoubleDoubleFunction minusMult(double constant) {
        return plusMult(-constant);
    }


    /** Constructs a function that returns <tt>a % b</tt>. <tt>a</tt> is a variable, <tt>b</tt> is fixed. */
    public static DoubleFunction mod(final double b) {
        return new DoubleFunction() {

            @Override
            public double apply(double a) {
                return a % b;
            }
        };
    }

    /** Constructs a function that returns <tt>a * b</tt>. <tt>a</tt> is a variable, <tt>b</tt> is fixed. */
    //public static DoubleFunction mult(double b) {
    //    return new Mult(b);
    /*
    return new DoubleFunction() {
      public final double apply(double a) { return a * b; }
    };
    */
    //}

    /** Constructs a function that returns <tt>a + b</tt>. <tt>a</tt> is a variable, <tt>b</tt> is fixed. */
    public static DoubleFunction plus(final double b) {
        return new DoubleFunction() {

            @Override
            public double apply(double a) {
                return a + b;
            }
        };
    }

    /**
     * Constructs a function that returns <tt>a + b*constant</tt>. <tt>a</tt> and <tt>b</tt> are variables,
     * <tt>constant</tt> is fixed.
     */
    public static DoubleDoubleFunction plusMult(double constant) {
        return new PlusMult(constant);
    /*
    return new DoubleDoubleFunction() {
      public final double apply(double a, double b) { return a + b*constant; }
    };
    */
    }

    /** Constructs a function that returns <tt>Math.pow(a,b)</tt>. <tt>a</tt> is a variable, <tt>b</tt> is fixed. */
    public static DoubleFunction pow(final double b) {
        return new DoubleFunction() {

            @Override
            public double apply(double a) {
                return Math.pow(a, b);
            }
        };
    }

    /**
     * Constructs a function that returns the number rounded to the given precision;
     * <tt>Math.rint(a/precision)*precision</tt>. Examples:
     * <pre>
     * precision = 0.01 rounds 0.012 --> 0.01, 0.018 --> 0.02
     * precision = 10   rounds 123   --> 120 , 127   --> 130
     * </pre>
     */
    public static DoubleFunction round(final double precision) {
        return new DoubleFunction() {
            @Override
            public double apply(double a) {
                return Math.rint(a / precision) * precision;
            }
        };
    }

    /**
     * Constructs a function that returns <tt>function.apply(b,a)</tt>, i.e. applies the function with the first operand
     * as second operand and the second operand as first operand.
     *
     * @param function a function taking operands in the form <tt>function.apply(a,b)</tt>.
     * @return the binary function <tt>function(b,a)</tt>.
     */
    public static DoubleDoubleFunction swapArgs(final DoubleDoubleFunction function) {
        return new DoubleDoubleFunction() {
            @Override
            public double apply(double a, double b) {
                return function.apply(b, a);
            }
        };
    }
}