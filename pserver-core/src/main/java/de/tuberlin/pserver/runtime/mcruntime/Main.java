package de.tuberlin.pserver.runtime.mcruntime;


public class Main {

    public static void main(String[] args) {

        final MCRuntime mcRuntime = MCRuntime.INSTANCE;

        //mcRuntime.create(4);

        // -----------------------------------------------------------------------
        /*
        { // Simple Parallel Body, DOP 1
            final int dop = 1;

            final AtomicInteger counter = new AtomicInteger(0);

            final Runnable prg = () -> {

                try {

                    Parallel.Do(dop, counter::incrementAndGet);

                } catch(Exception e) {
                    throw new IllegalStateException(e);
                }
            };

            prg.run();

            Preconditions.checkState(counter.get() == 1);

            System.out.println("FINISHED TEST (1)");
        }

        // -----------------------------------------------------------------------

        { // Simple Parallel Body, DOP 4
            final int dop = 4;

            final AtomicInteger counter = new AtomicInteger(0);

            final Runnable prg = () -> {

                try {

                    for (int i = 0; i < 8000; ++i) {

                        Parallel.Do(dop, counter::incrementAndGet);

                        Preconditions.checkState(dop == counter.get());

                        counter.set(0);
                    }

                } catch(Exception e) {
                    throw new IllegalStateException(e);
                }
            };

            prg.run();

            System.out.println("FINISHED TEST (2)");
        }

        // -----------------------------------------------------------------------

        { // Sequential Nesting Parallel Body, DOP 1
            final int dop = 1;

            final Runnable prg = () -> {

                try {

                    Parallel.Do(dop, () -> {

                        //System.out.println("[" + id() + "]" + " level 0");

                        Parallel.Do(dop, () -> {

                            //System.out.println("[" + id() + "]" + " level 1");

                            Parallel.Do(dop, () -> {

                                //System.out.println("[" + id() + "]" + " level 2");

                                Parallel.Do(dop, () -> {

                                    //System.out.println("[" + id() + "]" + " level 3");

                                });
                            });
                        });
                    });

                } catch(Exception e) {
                    throw new IllegalStateException(e);
                }
            };

            prg.run();

            System.out.println("FINISHED TEST (3)");
        }

        // -----------------------------------------------------------------------

        { // Sequential Nesting Parallel Body, DOP 4
            final int dop = 4;

            final Runnable prg = () -> {

                try {

                    for (int i = 0; i < 8000; ++i) {

                        Parallel.Do(dop, () -> {

                            //System.out.println("[" + id() + "]" + " level 0");

                            Parallel.Do(dop - 1, () -> {

                                //System.out.println("[" + id() + "]" + " level 1");

                                Parallel.Do(dop - 2, () -> {

                                    //System.out.println("[" + id() + "]" + " level 2");

                                    Parallel.Do(dop - 3, () -> {

                                        //System.out.println("[" + id() + "]" + " level 3");
                                    });
                                });
                            });
                        });
                    }

                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            };

            prg.run();

            System.out.println("FINISHED TEST (4)");
        }

        // -----------------------------------------------------------------------

        { // Equal Parallel Nesting Parallel Body, DOP 4
            final int dop = 4;

            final Runnable prg = () -> {

                try {

                    for (int i = 0; i < 8000; ++i) {

                        Parallel.Do(dop, () -> {

                            Parallel.Do(2, () -> {

                                //System.out.println("[" + id() + "]" + " BLOCK A");

                            });


                            Parallel.Do(2, () -> {

                                //System.out.println("[" + id() + "]" + " BLOCK B");

                            });
                        });
                    }

                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            };

            prg.run();

            System.out.println("FINISHED TEST (5)");
        }*/

        // -----------------------------------------------------------------------

        /*{ // Equal Parallel Nesting Parallel Body, DOP 4
            final int dop = 4;

            final Runnable prg = () -> {

                try {

                    for (int i = 0; i < 8000; ++i) {

                        Parallel.Do(dop, () -> {

                            Parallel.Do(1, () -> {

                                //System.out.println("[" + id() + "]" + " BLOCK A");

                            });


                            Parallel.Do(3, () -> {

                                //System.out.println("[" + id() + "]" + " BLOCK B");

                            });
                        });
                    }

                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            };

            prg.run();

            System.out.println("FINISHED TEST (6)");
        }*/


        // -----------------------------------------------------------------------

        { // Equal Parallel Nesting Parallel Body, DOP 4

            final Runnable prg = () -> {

                try {

                    Parallel.Do(4, () -> {

                        System.out.println("Hello at core " + Parallel.id());

                    });


                    /*Parallel.For(0, 100, (i) -> {

                        System.out.println("Iteration " + i + " at core " + Parallel.id());

                    });*/


                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            };


            for (int i = 0; i < 100; i++) {

            }

            prg.run();

            System.out.println("FINISHED TEST (7)");
        }

        mcRuntime.deactivate();
    }
}
