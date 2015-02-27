package de.tuberlin.pserver.examples.local;

import de.tuberlin.pserver.client.PServerClient;
import de.tuberlin.pserver.core.memory.Buffer;
import de.tuberlin.pserver.core.infra.ClusterSimulator;
import de.tuberlin.pserver.app.PServerInvokeable;
import de.tuberlin.pserver.app.dht.DHT;
import de.tuberlin.pserver.app.dht.Key;
import de.tuberlin.pserver.app.dht.Value;
import de.tuberlin.pserver.node.PServerNode;
import org.apache.log4j.ConsoleAppender;

import java.util.Random;
import java.util.UUID;

public final class LocalDHTTestJob {

    private LocalDHTTestJob() {}

    // ---------------------------------------------------
    // app Jobs.
    // ---------------------------------------------------

    public static final class DHTTestJob implements PServerInvokeable {

        private boolean detectDuplicates(final int[] a) {
            boolean duplicates = false;
            for (int j = 0; j < a.length; ++j)
                for (int k = j + 1; k < a.length; ++k)
                    if (k != j && a[k] == a[j])
                        duplicates = true;
            return duplicates;
        }

        @Override
        public void invoke(final int instanceID) {
            final Random r = new Random();
            final UUID uid = UUID.fromString("31bf55a7-2195-4d11-8ebf-0d030032fede");
            if (instanceID == 0) {
                DHT.getInstance().put(Key.newKey(uid), Value.newValue(false, Value.MAX_SIZE * 4));
            }
            final Key key = DHT.getInstance().getKey(uid);
            while (true) {
                final int numOfAccessedSegments = r.nextInt(10);
                final int segmentIDs[] = new int[numOfAccessedSegments];
                for (int i = 0; i < numOfAccessedSegments; ++i)
                    segmentIDs[i] = r.nextInt((20480 * 4) - 1);

                if (!detectDuplicates(segmentIDs)) {
                    final Value.Segment[] segments = DHT.getInstance().get(key, segmentIDs);
                    for (int i = 0; i < segmentIDs.length; ++i) {
                        if (segments[i] == null)
                            throw new IllegalStateException("segment == null => segmentID = " + segmentIDs[i]);
                    }
                    for (int i = 0; i < segmentIDs.length; ++i) {
                        final Buffer buffer = new Buffer(segments[i].data);
                        final int randOffset = r.nextInt(Key.DEFAULT_SEGMENT_SIZE - 10);
                        buffer.putInt(randOffset, r.nextInt());
                    }
                    DHT.getInstance().put(key, segments);
                }
            }
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender());
        new ClusterSimulator(PServerNode.class, true, 4);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        new PServerClient().execute(DHTTestJob.class);
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
