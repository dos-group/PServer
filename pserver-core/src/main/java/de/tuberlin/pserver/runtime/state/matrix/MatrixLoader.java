package de.tuberlin.pserver.runtime.state.matrix;

import de.tuberlin.pserver.compiler.StateDescriptor;
import de.tuberlin.pserver.math.matrix.*;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.filesystem.FileDataIterator;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.filesystem.records.Record;
import de.tuberlin.pserver.runtime.state.matrix.entries.Entry;
import de.tuberlin.pserver.runtime.state.matrix.entries.MutableEntryImpl;
import de.tuberlin.pserver.runtime.state.matrix.entries.ReusableEntry;
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

        abstract public void putRecord(Record record, MatrixBase dataMatrix, MatrixBase labelMatrix);

        public void done(MatrixBase dataMatrix) {}
    }

    // ---------------------------------------------------

    private static final class MatrixLoaderFactory {
        public static MatrixLoaderStrategy createLoader(StateDescriptor state) {
            if (CSRMatrix32F.class.isAssignableFrom(state.stateType))
                return new CSRMatrix32FLoaderStrategy(state);
            if (Matrix32F.class.isAssignableFrom(state.stateType))
                return new Matrix32FLoaderStrategy(state);
            if (Matrix64F.class.isAssignableFrom(state.stateType))
                return new Matrix64FLoaderStrategy(state);
            throw new IllegalStateException();
        }
    }

    // ---------------------------------------------------

    private final static class Matrix32FLoaderStrategy extends MatrixLoaderStrategy {

        private final ReusableEntry<Float> reusable = new MutableEntryImpl<>(-1, -1, Float.NaN);

        public Matrix32FLoaderStrategy(StateDescriptor state)  { super(state); }

        @Override
        public void putRecord(Record record, MatrixBase dataMatrix, MatrixBase labelMatrix) {
            while (record.hasNext()) { // Iterate through entries in record...
                final Entry entry = record.next(reusable);
                if (entry.getRow() >= state.rows || entry.getCol() >= state.cols)
                    continue;
                if (labelMatrix != null && entry.getCol() == 0) // Label always on first column.
                    ((Matrix32F) labelMatrix).set(record.getRow(), entry.getCol(), record.getLabel().floatValue());
                else
                    ((Matrix32F) dataMatrix).set(entry.getRow(), entry.getCol(), entry.getValue().floatValue());
            }
        }
    }

    private final static class Matrix64FLoaderStrategy extends MatrixLoaderStrategy {

        private final ReusableEntry<Double> reusable = new MutableEntryImpl<>(-1, -1, Double.NaN);

        public Matrix64FLoaderStrategy(StateDescriptor state)  { super(state); }

        @Override
        public void putRecord(Record record, MatrixBase dataMatrix, MatrixBase labelMatrix) {
            while (record.hasNext()) { // Iterate through entries in record...
                final Entry entry = record.next(reusable);
                if (entry.getRow() >= state.rows || entry.getCol() >= state.cols)
                    continue;
                if (labelMatrix != null && entry.getCol() == 0) // Label always on first column.
                    ((Matrix64F) labelMatrix).set(record.getRow(), entry.getCol(), record.getLabel().doubleValue());
                else
                    ((Matrix64F) dataMatrix).set(entry.getRow(), entry.getCol(), entry.getValue().doubleValue());
            }
        }
    }

    private final static class CSRMatrix32FLoaderStrategy extends MatrixLoaderStrategy {

        private final ReusableEntry<Float> reusable = new MutableEntryImpl<>(-1, -1, Float.NaN);
        private final TIntFloatHashMap rowData = new TIntFloatHashMap();

        public CSRMatrix32FLoaderStrategy(StateDescriptor state)  { super(state); }

        @Override
        public void putRecord(Record record, MatrixBase dataMatrix, MatrixBase labelMatrix) {
            while (record.hasNext()) { // Iterate through entries in record...
                final Entry entry = record.next(reusable);
                if (entry.getRow() >= state.rows || entry.getCol() >= state.cols)
                    continue;
                if (labelMatrix != null && entry.getCol() == 0) // Label always on first column.
                    ((Matrix32F) labelMatrix).set(record.getRow(), entry.getCol(), record.getLabel().floatValue());
                else
                    rowData.put((int)(entry.getCol() % state.cols), entry.getValue().floatValue());
            }
            ((CSRMatrix32F) dataMatrix).addRow(rowData);
            rowData.clear();
        }

        @Override
        public void done(MatrixBase dataMatrix) {
            ((CSRMatrix32F) dataMatrix).build();
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final ProgramContext programContext;

    private final FileSystemManager fileManager;

    private final List<Triple<StateDescriptor, Matrix, FileDataIterator>> loadingTasks;

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

    public void add(StateDescriptor stateDescriptor, Matrix stateObj) {
        FileDataIterator fileIterator = fileManager.createFileIterator(programContext, stateDescriptor);
        loadingTasks.add(Triple.of(stateDescriptor, stateObj, fileIterator));
    }

    @SuppressWarnings("unchecked")
    public void load() {
        programContext.runtimeContext.fileManager.computeInputSplitsForRegisteredFiles();
        for (Triple<StateDescriptor, Matrix, FileDataIterator> task : loadingTasks) {
            StateDescriptor state = task.getLeft();
            Matrix dataMatrix = task.getMiddle();
            FileDataIterator<Record> fileIterator = task.getRight();
            Matrix labelMatrix = null;
            if (!"".equals(state.label)) {
                labelMatrix = programContext.runtimeContext.runtimeManager.getDHT(state.label);
            }
            final MatrixLoaderStrategy loader = MatrixLoaderFactory.createLoader(state);
            while (fileIterator.hasNext())
                loader.putRecord(fileIterator.next(), dataMatrix, labelMatrix);
            loader.done(dataMatrix);
        }
    }
}