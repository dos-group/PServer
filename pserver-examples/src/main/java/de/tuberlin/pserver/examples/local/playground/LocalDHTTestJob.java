package de.tuberlin.pserver.examples.local.playground;

import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.app.dht.DHT;
import de.tuberlin.pserver.app.dht.Key;
import de.tuberlin.pserver.app.dht.valuetypes.ByteBufferValue;
import de.tuberlin.pserver.client.PServerClient;
import de.tuberlin.pserver.client.PServerClientFactory;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.config.IConfigFactory;
import de.tuberlin.pserver.core.infra.ClusterSimulator;
import de.tuberlin.pserver.math.experimental.memory.Buffer;
import de.tuberlin.pserver.node.PServerMain;
import org.apache.log4j.ConsoleAppender;

import java.util.Random;
import java.util.UUID;

public final class LocalDHTTestJob {

    private LocalDHTTestJob() {}

    // ---------------------------------------------------
    // HDFS Commands.
    // ---------------------------------------------------

    /**
     * ./start-all.sh
     * ./stop-all.sh
     *
     */

    // ---------------------------------------------------
    // Jobs.
    // ---------------------------------------------------

    public static final class DHTTestJob extends PServerJob {

        private boolean detectDuplicates(final int[] a) {
            boolean duplicates = false;
            for (int j = 0; j < a.length; ++j)
                for (int k = j + 1; k < a.length; ++k)
                    if (k != j && a[k] == a[j])
                        duplicates = true;
            return duplicates;
        }

        @Override
        public void compute() {
            final Random r = new Random();
            final UUID uid = UUID.fromString("31bf55a7-2195-4d11-8ebf-0d030032fede");
            if (ctx.instanceID == 0) {
                ctx.dht.put(Key.newKey(uid, "test1", Key.DistributionMode.DISTRIBUTED), ByteBufferValue.newValue(false, ByteBufferValue.MAX_SIZE * 4));
            }
            final Key key = ctx.dht.getKey(uid);
            while (true) {
                final int numOfAccessedSegments = r.nextInt(10);
                final int segmentIDs[] = new int[numOfAccessedSegments];
                for (int i = 0; i < numOfAccessedSegments; ++i)
                    segmentIDs[i] = r.nextInt((20480 * 4) - 1);

                if (!detectDuplicates(segmentIDs)) {
                    final ByteBufferValue.Segment[] segments = DHT.getInstance().get(key, segmentIDs);
                    for (int i = 0; i < segmentIDs.length; ++i) {
                        if (segments[i] == null)
                            throw new IllegalStateException("segment == null => segmentID = " + segmentIDs[i]);
                    }
                    for (int i = 0; i < segmentIDs.length; ++i) {
                        final Buffer buffer = new Buffer(segments[i].data);
                        final int randOffset = r.nextInt(ByteBufferValue.DEFAULT_SEGMENT_SIZE - 10);
                        buffer.putInt(randOffset, r.nextInt());
                    }
                    ctx.dht.put(key, segments);
                }
            }
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender());

        final ClusterSimulator simulator = new ClusterSimulator(
                IConfigFactory.load(IConfig.Type.PSERVER_SIMULATION),
                PServerMain.class
        );

        PServerClientFactory.createPServerClient().execute(DHTTestJob.class);

        final PServerClient client = PServerClientFactory.createPServerClient();

        client.execute(DHTTestJob.class);

        client.shutdown();

        simulator.shutdown();
    }
}
