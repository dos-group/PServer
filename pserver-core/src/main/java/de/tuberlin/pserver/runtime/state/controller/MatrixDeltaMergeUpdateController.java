package de.tuberlin.pserver.runtime.state.controller;


import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import de.tuberlin.pserver.commons.compression.Compressor;
import de.tuberlin.pserver.compiler.ProgramContext;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.dht.types.EmbeddedDHTObject;
import de.tuberlin.pserver.runtime.state.update.MatrixDeltaUpdate;

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

    public MatrixDeltaMergeUpdateController(final ProgramContext programContext,
                                            final String stateName,
                                            final Matrix matrix) {
        super(programContext, stateName);

        this.stateMatrix = Preconditions.checkNotNull(matrix);

        this.buffer = new byte[(int)(matrix.rows() * matrix.cols()) * Doubles.BYTES];

        this.matrixDelta = new EmbeddedDHTObject<>(new MatrixDeltaUpdate(programContext.runtimeContext.numOfCores));

        programContext.runtimeContext.dataManager.putObject(stateName + "-Delta", matrixDelta);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void publishUpdate() throws Exception {
/*
        Preconditions.checkState(filter != null);

        Preconditions.checkState(filter instanceof MatrixUpdateFilter);

        final MatrixUpdateFilter matrixUpdateFilter = (MatrixUpdateFilter) filter;

        sc.CF.syncSlots();

        sc.CF.loop().parExe(stateMatrix, (e, i, j, v) -> {

            final double newVal = stateMatrix.get(i, j);

            final long bufferOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET + ((i * stateMatrix.cols() + j) * Double.BYTES);

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

        sc.CF.loop().parExe(stateMatrix, (e, i, j, v) -> {

            final long bufferOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET + (i * stateMatrix.cols() + j) * Double.BYTES;

            UnsafeOp.unsafe.putDouble(buffer, bufferOffset, v);
        });

        sc.CF.syncSlots();*/
    }

    @Override
    public void pullUpdate() throws Exception {
/*
        Preconditions.checkState(merger != null);

        Preconditions.checkState(merger instanceof MatrixUpdateMerger);

        final MatrixUpdateMerger matrixUpdateMerger = (MatrixUpdateMerger)merger;

        sc.CF.syncSlots();

        sc.CF.parUnit(0).exe( () -> {

            final DataManager dataManager = sc.runtimeContext.dataManager;

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

                sc.CF.loop().parExe(stateMatrix, (e, i, j, v) -> {

                    final long bufferOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET + ((i * stateMatrix.cols() + j) * Double.BYTES);

                    final double remoteVal = UnsafeOp.unsafe.getDouble(buffer, bufferOffset);

                    if (remoteVal != Double.NaN)
                        matrixUpdateMerger.mergeElement(i, j, v, remoteVal);
                });
            }
        }

        sc.CF.syncSlots();

        sc.CF.parUnit(0).exe(() -> sc.programContext.delete(stateName + "-Remote-Matrix-Delta-List"));*/
    }
}
