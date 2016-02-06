package de.tuberlin.pserver.runtime.state.matrix;

import de.tuberlin.pserver.compiler.StateDescriptor;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.Matrix64F;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.filesystem.FileDataIterator;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.filesystem.records.Entry;
import de.tuberlin.pserver.runtime.filesystem.records.Record;
//import de.tuberlin.pserver.runtime.state.matrix.entries.Entry;
//import de.tuberlin.pserver.runtime.state.matrix.entries.MutableEntryImpl;
//import de.tuberlin.pserver.runtime.state.matrix.entries.ReusableEntry;
import org.apache.commons.lang3.tuple.Triple;

import java.util.ArrayList;
import java.util.List;

public final class MatrixLoader {

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

            //final ReusableEntry reusable = new MutableEntryImpl(-1, -1, Double.NaN);
            Entry entry = new Entry(-1, -1, Double.NaN);
            while (fileIterator.hasNext()) {

                final Record record = fileIterator.next();

                // iterate through entries in record
                while (record.hasNext()) {
                    //final Entry entry = record.next(reusable);
                    entry = record.next(entry);

                    if (entry.getRow() >= state.rows || entry.getCol() >= state.cols)
                        continue;

                    if (labelMatrix != null && entry.getCol() == 0) { // Label always on first column.
                        if (Matrix32F.class.isAssignableFrom(labelMatrix.getClass()))
                            labelMatrix.set(record.getRow(), entry.getCol(), record.getLabel().floatValue());
                        if (Matrix64F.class.isAssignableFrom(labelMatrix.getClass()))
                            labelMatrix.set(record.getRow(), entry.getCol(), record.getLabel().doubleValue());
                    }

                    if (Matrix32F.class.isAssignableFrom(dataMatrix.getClass()))
                        dataMatrix.set(entry.getRow(), entry.getCol(), entry.getValue().floatValue());
                    if (Matrix64F.class.isAssignableFrom(dataMatrix.getClass()))
                        dataMatrix.set(entry.getRow(), entry.getCol(), entry.getValue().doubleValue());
                }
            }
        }
    }
}