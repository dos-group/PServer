package de.tuberlin.pserver.runtime.state.controller;


import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import de.tuberlin.pserver.commons.compression.Compressor;
import de.tuberlin.pserver.commons.unsafe.UnsafeOp;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.SlotContext;
import de.tuberlin.pserver.runtime.dht.DHTObject;
import de.tuberlin.pserver.runtime.dht.types.EmbeddedDHTObject;
import de.tuberlin.pserver.runtime.state.filter.MatrixUpdateFilter;
import de.tuberlin.pserver.runtime.state.merger.MatrixUpdateMerger;
import de.tuberlin.pserver.runtime.state.update.MatrixDeltaUpdate;
import sun.misc.Unsafe;

import java.util.ArrayList;
import java.util.List;

public class MatrixDeltaMergeUpdateController extends RemoteUpdateController {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Compressor compressor = Compressor.Factory.create(Compressor.CompressionType.LZ4_COMPRESSION);

    private final Matrix stateMatrix;

    private final byte[] buffer;

    private final EmbeddedDHTObject<MatrixDeltaUpdate> matrixDelta;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MatrixDeltaMergeUpdateController(final SlotContext slotContext,
                                            final String stateName,
                                            final Matrix matrix) {
        super(slotContext, stateName);

        this.stateMatrix = Preconditions.checkNotNull(matrix);

        this.buffer = new byte[(int)(matrix.numRows() * matrix.numCols()) * Doubles.BYTES];

        this.matrixDelta = new EmbeddedDHTObject<>(new MatrixDeltaUpdate(slotContext.programContext.perNodeDOP));

        slotContext.programContext.runtimeContext.dataManager.putObject(stateName + "-Delta", matrixDelta);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void publishUpdate(final SlotContext sc) throws Exception {

        Preconditions.checkState(filter != null);

        Preconditions.checkState(filter instanceof MatrixUpdateFilter);

        final MatrixUpdateFilter matrixUpdateFilter = (MatrixUpdateFilter) filter;

        sc.CF.syncSlots();

        sc.CF.iterate().parExe(stateMatrix, (e, i, j, v) -> {

            final double newVal = stateMatrix.get(i, j);

            final long bufferOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET + ((i * stateMatrix.numCols() + j) * Double.BYTES);

            final double oldVal = UnsafeOp.unsafe.getDouble(buffer, bufferOffset);

            final double deltaVal = matrixUpdateFilter.filter(i, j, oldVal, newVal) ? newVal : Double.NaN;

            UnsafeOp.unsafe.putDouble(buffer, bufferOffset, deltaVal);
        });

        final int offset = (buffer.length / sc.programContext.perNodeDOP) * sc.slotID;

        int length = buffer.length / sc.programContext.perNodeDOP;

        length = (sc.slotID == sc.programContext.perNodeDOP - 1) ? (length + (buffer.length % 2)) : length;

        synchronized (matrixDelta.lock) {

            matrixDelta.object.setDelta(sc.slotID, compressor.compress(buffer, offset, length)); // TODO: Creates a new buffer...
        }

        sc.CF.iterate().parExe(stateMatrix, (e, i, j, v) -> {

            final long bufferOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET + (i * stateMatrix.numCols() + j) * Double.BYTES;

            UnsafeOp.unsafe.putDouble(buffer, bufferOffset, v);
        });

        sc.CF.syncSlots();
    }

    @Override
    public void pullUpdate(final SlotContext sc) throws Exception {

        Preconditions.checkState(merger != null);

        Preconditions.checkState(merger instanceof MatrixUpdateMerger);

        final MatrixUpdateMerger matrixUpdateMerger = (MatrixUpdateMerger)merger;

        sc.CF.syncSlots();

        sc.CF.select().slot(0).exe( () -> {

            final DataManager dataManager = sc.programContext.runtimeContext.dataManager;

            final DHTObject[] dhtObjects = dataManager.pullFrom(stateName + "-Delta", dataManager.remoteNodeIDs);

            Preconditions.checkState(dhtObjects.length > 0);

            final List<MatrixDeltaUpdate> remoteMatrixDeltaUpdates = new ArrayList<>();

            for (final DHTObject obj : dhtObjects) {

                Preconditions.checkState(((EmbeddedDHTObject) obj).object instanceof MatrixDeltaUpdate);

                final MatrixDeltaUpdate remoteMatrixDeltaUpdate = (MatrixDeltaUpdate)((EmbeddedDHTObject) obj).object;

                remoteMatrixDeltaUpdates.add(remoteMatrixDeltaUpdate);
            }

            sc.programContext.put(stateName + "-Remote-Matrix-Delta-List", remoteMatrixDeltaUpdates);
        });

        sc.CF.syncSlots();

        @SuppressWarnings("unchecked")
        final List<MatrixDeltaUpdate> remoteMatrixDeltaUpdates = (List<MatrixDeltaUpdate>) sc.programContext.get(stateName + "-Remote-Matrix-Delta-List");

        for (final MatrixDeltaUpdate remoteMatrixDeltaUpdate : remoteMatrixDeltaUpdates) {

            final byte[] compressedDelta = remoteMatrixDeltaUpdate.getDelta(sc.slotID);

            if (compressedDelta != null) {

                final int offset = (buffer.length / sc.programContext.perNodeDOP) * sc.slotID;

                int length = buffer.length / sc.programContext.perNodeDOP;

                length = (sc.slotID == sc.programContext.perNodeDOP - 1) ? (length + (buffer.length % 2)) : length;

                compressor.decompress(compressedDelta, 0, buffer, offset, length);

                sc.CF.iterate().parExe(stateMatrix, (e, i, j, v) -> {

                    final long bufferOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET + ((i * stateMatrix.numCols() + j) * Double.BYTES);

                    final double remoteVal = UnsafeOp.unsafe.getDouble(buffer, bufferOffset);

                    if (remoteVal != Double.NaN)
                        matrixUpdateMerger.mergeElement(i, j, v, remoteVal);
                });
            }
        }

        sc.CF.syncSlots();

        sc.CF.select().slot(0).exe(() -> sc.programContext.delete(stateName + "-Remote-Matrix-Delta-List"));
    }
}
