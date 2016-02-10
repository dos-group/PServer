package de.tuberlin.pserver.runtime.state.matrix;

import de.tuberlin.pserver.compiler.StateDescriptor;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.filesystem.FileDataIterator;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.filesystem.records.Record;
import de.tuberlin.pserver.types.matrix.f32.Matrix32F;
import de.tuberlin.pserver.types.matrix.f32.entries.Entry32F;
import de.tuberlin.pserver.types.matrix.f32.entries.MutableEntryImpl32F;
import de.tuberlin.pserver.types.matrix.f32.entries.ReusableEntry32F;
import de.tuberlin.pserver.types.matrix.f32.sparse.CSRMatrix32F;
import gnu.trove.map.hash.TIntFloatHashMap;
import org.apache.commons.lang3.tuple.Triple;

import java.util.ArrayList;
import java.util.List;

public final class MatrixLoader {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private static abstract class MatrixLoaderStrategy {

        protected final StateDescriptor state;

        public MatrixLoaderStrategy(StateDescriptor state)  { this.state = state; }

        public void done(Matrix32F dataMatrix) {}

        abstract public void putRecord(Record record, Matrix32F dataMatrix, Matrix32F labelMatrix);

        // Factory Method.
        public static MatrixLoaderStrategy createLoader(StateDescriptor state) {
            if (CSRMatrix32F.class.isAssignableFrom(state.stateType))
                return new CSRMatrix32LoaderStrategy(state);
            if (Matrix32F.class.isAssignableFrom(state.stateType))
                return new Matrix32LoaderStrategy(state);
            throw new IllegalStateException();
        }
    }

    // ---------------------------------------------------

    private final static class Matrix32LoaderStrategy extends MatrixLoaderStrategy {

        private final ReusableEntry32F reusable = new MutableEntryImpl32F(-1, -1, Float.NaN);

        public Matrix32LoaderStrategy(StateDescriptor state)  { super(state); }

        @Override
        public void putRecord(Record record, Matrix32F dataMatrix, Matrix32F labelMatrix) {
            while (record.hasNext()) { // Iterate through entries in record...
                final Entry32F entry = record.next(reusable);
                if (entry.getRow() > state.rows || entry.getCol() > state.cols)
                    return;
                if (labelMatrix != null && entry.getCol() == 0) // Label always on first column.
                    labelMatrix.set(record.getRow() - labelMatrix.shape().rowOffset, entry.getCol(), record.getLabel());
                else
                    dataMatrix.set(entry.getRow() - dataMatrix.shape().rowOffset, entry.getCol() - ((labelMatrix != null) ? 1 : 0), entry.getValue());
            }
        }
    }

    private final static class CSRMatrix32LoaderStrategy extends MatrixLoaderStrategy {

        private final ReusableEntry32F reusable = new MutableEntryImpl32F(-1, -1, Float.NaN);
        private final TIntFloatHashMap rowData = new TIntFloatHashMap();

        public CSRMatrix32LoaderStrategy(StateDescriptor state)  { super(state); }

        @Override
        public void putRecord(Record record, Matrix32F dataMatrix, Matrix32F labelMatrix) {
            while (record.hasNext()) { // Iterate through entries in record...
                final Entry32F entry = record.next(reusable);
                if (entry.getRow() > state.rows || entry.getCol() > state.cols)
                    continue;
                if (labelMatrix != null && entry.getCol() == 0) // Label always on first column.
                    labelMatrix.set(record.getRow(), entry.getCol(), record.getLabel()); // TODO: VERIFY THAT!!!!!!!!!!!!!!!!!!!!
                else {
                    rowData.put((int) (entry.getCol() - ((labelMatrix != null) ? 1 : 0) % state.cols), entry.getValue());
                }
            }
            ((CSRMatrix32F) dataMatrix).addRow(rowData);
            rowData.clear();
        }

        @Override
        public void done(Matrix32F dataMatrix) {
            ((CSRMatrix32F) dataMatrix).build();
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final ProgramContext programContext;

    private final FileSystemManager fileManager;

    private final List<Triple<StateDescriptor, Matrix32F, FileDataIterator>> loadingTasks;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MatrixLoader(ProgramContext programContext) {
        this.programContext = programContext;
        this.fileManager    = programContext.runtimeContext.fileManager;
        this.loadingTasks   = new ArrayList<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void add(StateDescriptor stateDescriptor, Matrix32F stateObj) {
        FileDataIterator fileIterator = fileManager.createFileIterator(programContext, stateDescriptor);
        loadingTasks.add(Triple.of(stateDescriptor, stateObj, fileIterator));
    }

    @SuppressWarnings("unchecked")
    public void load() {
        programContext.runtimeContext.fileManager.computeInputSplitsForRegisteredFiles();
        for (Triple<StateDescriptor, Matrix32F, FileDataIterator> task : loadingTasks) {
            StateDescriptor state = task.getLeft();
            Matrix32F dataMatrix = task.getMiddle();
            FileDataIterator<Record> fileIterator = task.getRight();
            Matrix32F labelMatrix = null;
            if (!"".equals(state.label)) {
                labelMatrix = programContext.runtimeContext.runtimeManager.getDHT(state.label);
            }
            final MatrixLoaderStrategy loader = MatrixLoaderStrategy.createLoader(state);
            while (fileIterator.hasNext())
                loader.putRecord(fileIterator.next(), dataMatrix, labelMatrix);
            loader.done(dataMatrix);
        }
    }
}