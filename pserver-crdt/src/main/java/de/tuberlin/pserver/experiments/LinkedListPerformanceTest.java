package de.tuberlin.pserver.experiments;


import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.radt.list.LinkedList;

import java.io.Serializable;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;


public class LinkedListPerformanceTest extends Program {
    private static final String RADT_ID = "linkedList";
    private static final int NUM_NODES = 3;
    private static final int NUM_ELEMENTS = 100;
    private static final int NUM_OPERATIONS = 10000;
    private static final int MIN_ELEMENTS = 100;

    enum Type {
        INSERT,
        UPDATE,
        DELETE
    };

    private class Command {
        Type opType;
        int index;
        int value;

        public Command(Type opType, int index, int value) {
            this.opType = opType;
            this.index = index;
            this.value = value;
        }
    }

    @Unit
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            LinkedList<Integer> list = new LinkedList<>(RADT_ID, NUM_NODES, programContext);
            Random rand = new Random(Calendar.getInstance().getTimeInMillis());
            List<Command> buffer = new java.util.LinkedList<>();



            System.out.println("[TEST] " + programContext.nodeID + " Filling operation buffer");

            /*
            int num_inserts = 0;
            int num_deletes = 0;
            float p = 0;

            for(int i = 0; i < NUM_OPERATIONS; i++) {
                if(num_inserts - num_deletes < MIN_ELEMENTS) {
                    buffer.add(new Command(Type.INSERT, 0, rand.nextInt()));
                    num_inserts++;
                }
                else {
                    p = rand.nextFloat();
                    if(p < 0.5) {
                        buffer.add(new Command(Type.INSERT, rand.nextInt(MIN_ELEMENTS), rand.nextInt()));
                        num_inserts++;
                    }
                    else if(p < 0.75) {
                        buffer.add(new Command(Type.UPDATE, rand.nextInt(MIN_ELEMENTS), rand.nextInt()));
                    }
                    else {
                        buffer.add(new Command(Type.DELETE, rand.nextInt(MIN_ELEMENTS), 0));
                        num_deletes++;
                    }
                }
            }
            */


            // 1. Fill the whole list
            for(int i = 0; i < NUM_ELEMENTS; i++) {
                buffer.add(new Command(Type.INSERT, i, rand.nextInt()));
            }

            // 2. Do some updates
            for(int i = 0; i < (NUM_ELEMENTS/NUM_NODES)/2; i++) {
                buffer.add(new Command(Type.UPDATE, rand.nextInt(NUM_ELEMENTS), rand.nextInt()));
            }

            // 3. Do some deletions
            for(int i = 0; i< (NUM_ELEMENTS/NUM_NODES)/4; i++) {
                buffer.add(new Command(Type.DELETE, rand.nextInt(NUM_ELEMENTS/4), rand.nextInt()));
            }

            UnitMng.barrier(UnitMng.GLOBAL_BARRIER);
            final long startTime = Calendar.getInstance().getTimeInMillis();

            System.out.println("[TEST] " + programContext.nodeID + " Starting operations");

            for(Command com : buffer) {
                switch(com.opType) {
                    case INSERT:
                        list.insert(com.index, com.value);
                        break;
                    case UPDATE:
                        list.update(com.index, com.value);
                        break;
                    case DELETE:
                        list.delete(com.index);
                        break;
                    default:
                        throw new RuntimeException("Unknown operation type " + com.opType);
                }
            }

            final long intermediateTime = Calendar.getInstance().getTimeInMillis();

            list.finish();

            long stopTime = Calendar.getInstance().getTimeInMillis();
            System.out.println("[TEST] " + programContext.nodeID + " Finished.");
            System.out.println("Size: " + list.size());
            System.out.println("Check: " + list.getList().size());

            System.out.println("[TEST] Time: " + (stopTime - startTime) + "ms");

            result(list.getList().toArray(), stopTime - startTime, stopTime - intermediateTime,list.getTombstones());

            //System.out.println("[DEBUG] LinkedList of node " + programContext.runtimeContext.nodeID + ": " + list.getList());
            //System.out.println("[DEBUG] Tombstones in LinkedList of node " + programContext.runtimeContext.nodeID + ": " + list.getTombstones());
            System.out.println("[DEBUG] Queue of LinkedList of node " + programContext.runtimeContext.nodeID + ": " + list.getQueue().size());
        });
    }

    public static void test(String[] args) {
        final List<List<Serializable>> results = Lists.newArrayList();

        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", String.valueOf(NUM_NODES));
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx512m\"]");

        PServerExecutor.LOCAL
                .run(LinkedListPerformanceTest.class)
                .results(results)
                .done();

        Object[] firstResult = ((Object[])results.get(0).get(0));

        // Compare results of the CRDTs
        for(int i = 1; i < results.size(); i++) {
            assertArrayEquals("The resulting CRDTs are not identical", firstResult, (Object[])results.get(i).get(0));
        }
        System.out.println("\n[TEST] Passed: CRDTs have converged to consistent state.");

        System.out.println("\n[TEST] ***Results***");
        long avgTime = 0;
        for(int i = 0; i < NUM_NODES; i++) {
            System.out.println("[TEST] Node " + i + ": "
                    + "total time " + results.get(i).get(1) + "ms, "
                    + "replication time " + results.get(i).get(2) + "ms, "
                    + results.get(i).get(3) + " tombstones");
            avgTime += (long)results.get(i).get(1);
        }
        avgTime /= NUM_NODES;

        System.out.println("[TEST] Avg. execution time: " + avgTime + "ms");
    }
}
