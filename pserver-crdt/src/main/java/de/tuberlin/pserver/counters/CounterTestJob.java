package de.tuberlin.pserver.counters;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.ExecutionManager;
import de.tuberlin.pserver.runtime.MLProgram;

import java.util.Calendar;
import java.util.Random;

public class CounterTestJob extends MLProgram {

    @Override
    public void define(Program program) {
        super.define(program);

        program.process(() -> {
            CF.select().allNodes().allSlots().exe(() -> {
                Random r = new Random(Calendar.getInstance().getTimeInMillis());
                Counter c = new Counter(1, dataManager);

                dataManager.pushTo("Ready", 0, dataManager.remoteNodeIDs);

                dataManager.awaitEvent(ExecutionManager.CallType.SYNC, dataManager.remoteNodeIDs.length, "Ready", new DataManager.DataEventHandler() {
                    @Override
                    public void handleDataEvent(int srcNodeID, Object value) {

                    }
                });
                dataManager.pushTo("Ready", 0, dataManager.remoteNodeIDs);

                System.out.println("Check 1 [" + slotContext.programContext.runtimeContext.nodeID + "]");

                for(int i = 0; i < 10000; i++) {
                    //int value = r.nextInt(100);
                    c.add(1);
                    dataManager.pushTo("Operation", 1, dataManager.remoteNodeIDs);

                }

                System.out.println("Check 2 [" + slotContext.programContext.runtimeContext.nodeID + "]");


                dataManager.pushTo("Finished", 0, dataManager.remoteNodeIDs);

                System.out.println("Check 3 [" + slotContext.programContext.runtimeContext.nodeID + "]");


                /*while(c.getFinishedNodes().size() < dataManager.remoteNodeIDs.length) {
                    dataManager.pushTo("End", 0, dataManager.remoteNodeIDs);
                };*/

                dataManager.awaitEvent(ExecutionManager.CallType.SYNC, dataManager.remoteNodeIDs.length, "Finished", new DataManager.DataEventHandler() {
                    @Override
                    public void handleDataEvent(int srcNodeID, Object value) {
                    }
                });

                dataManager.pushTo("Finished", 0, dataManager.remoteNodeIDs);

                System.out.println("Check 4 [" + slotContext.programContext.runtimeContext.nodeID + "]");


                System.out.println("Count of node " + slotContext.programContext.runtimeContext.nodeID + ": " + c.getCount());
                System.out.println("Number of remote nodes: " + dataManager.remoteNodeIDs.length);
                System.out.println("Finished nodes: " + c.getFinishedNodes().size());

            });
        });


    }

    /*private class Operation implements Serializable {
        private Integer type;
        private Integer value;

        public Operation(int type, int value) {
            this.type = type;
            this.value = value;
        }

        public int getType() {
            return type;
        }

        public int getValue() {
            return value;
        }
    }*/


    public static void main(final String[] args) {

        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", "3");
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx512m\"]");

        PServerExecutor.LOCAL
                // Second param is number of slots (threads executing the job) per node,
                // should be 1 at the beginning.
                .run(CounterTestJob.class, 1)
                .done();
    }
}
