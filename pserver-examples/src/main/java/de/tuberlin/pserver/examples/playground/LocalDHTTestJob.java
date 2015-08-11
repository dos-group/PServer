package de.tuberlin.pserver.examples.playground;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.runtime.JobExecutable;
import de.tuberlin.pserver.runtime.dht.DHTKey;
import de.tuberlin.pserver.runtime.dht.DHTManager;
import de.tuberlin.pserver.runtime.dht.types.ByteBufferedDHTObject;
import de.tuberlin.pserver.runtime.memory.ManagedBuffer;

import java.util.Random;
import java.util.UUID;

public final class LocalDHTTestJob {

    private LocalDHTTestJob() {}

    // ---------------------------------------------------
    // Jobs.
    // ---------------------------------------------------

    public static final class DHTTestJob extends JobExecutable {

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
            if (slotContext.jobContext.nodeID == 0) {
                slotContext.jobContext.dht.put(DHTKey.newKey(uid, "test1", DHTKey.DistributionMode.DISTRIBUTED), ByteBufferedDHTObject.newValue(false, ByteBufferedDHTObject.MAX_SIZE * 4));
            }
            final DHTKey key = slotContext.jobContext.dht.getKey(uid);
            while (true) {
                final int numOfAccessedSegments = r.nextInt(10);
                final int segmentIDs[] = new int[numOfAccessedSegments];
                for (int i = 0; i < numOfAccessedSegments; ++i)
                    segmentIDs[i] = r.nextInt((20480 * 4) - 1);

                if (!detectDuplicates(segmentIDs)) {
                    final ByteBufferedDHTObject.Segment[] segments = DHTManager.getInstance().get(key, segmentIDs);
                    for (int i = 0; i < segmentIDs.length; ++i) {
                        if (segments[i] == null)
                            throw new IllegalStateException("segment == null => segmentID = " + segmentIDs[i]);
                    }
                    for (int i = 0; i < segmentIDs.length; ++i) {
                        final ManagedBuffer buffer = new ManagedBuffer(segments[i].data);
                        final int randOffset = r.nextInt(ByteBufferedDHTObject.DEFAULT_SEGMENT_SIZE - 10);
                        buffer.putInt(randOffset, r.nextInt());
                    }
                    slotContext.jobContext.dht.put(key, segments);
                }
            }
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        PServerExecutor.LOCAL
                .run(DHTTestJob.class)
                .done();
    }
}
