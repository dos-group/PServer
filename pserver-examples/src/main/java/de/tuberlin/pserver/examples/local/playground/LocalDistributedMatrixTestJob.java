package de.tuberlin.pserver.examples.local.playground;

import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.app.dht.Key;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.math.experimental.memory.Types;
import de.tuberlin.pserver.math.experimental.types.matrices.DistributedDenseMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public final class LocalDistributedMatrixTestJob {

    private LocalDistributedMatrixTestJob() {}

    // ---------------------------------------------------
    // Jobs.
    // ---------------------------------------------------

    public static final class DistributedMatrixTestJob extends PServerJob {

        private static final Logger LOG = LoggerFactory.getLogger(DistributedMatrixTestJob.class);

        @Override
        public void compute() {
            if (ctx.instanceID == 0) {

                final DistributedDenseMatrix m = DistributedDenseMatrix.create(
                        Key.newKey(UUID.fromString("31bf55a7-2195-4d11-8ebf-0d030032fede"), "test1", Key.DistributionMode.DISTRIBUTED),
                        4096 * 12,
                        4096,
                        Types.DOUBLE_TYPE_INFO,
                        DistributedDenseMatrix.PartitioningScheme.ROW_PARTITIONING
                );

                try { Thread.sleep(2000); } catch (Exception e) {}
                LOG.info("==> " + Types.toDouble(m.getElement(4096 * 12 - 1, 4096 - 1)));
                m.setElement(4096 * 12 - 1, 4096 - 1, Types.toByteArray(21345.53645));
                try { Thread.sleep(2000); } catch (Exception e) {}
                LOG.info("==> " + Types.toDouble(m.getElement(4096 * 12 - 1, 4096 - 1)));
            }
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        PServerExecutor.LOCAL
                .run(DistributedMatrixTestJob.class)
                .done();
    }
}