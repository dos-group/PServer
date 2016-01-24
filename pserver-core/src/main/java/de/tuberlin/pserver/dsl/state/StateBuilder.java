package de.tuberlin.pserver.dsl.state;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.utils.ParseUtils;
import de.tuberlin.pserver.compiler.StateDescriptor;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.math.matrix.MatrixBase;
import de.tuberlin.pserver.math.matrix.MatrixFormat;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.filesystem.FileFormat;
import de.tuberlin.pserver.runtime.state.matrix.partitioner.MatrixPartitioner;
import de.tuberlin.pserver.runtime.state.matrix.partitioner.RowPartitioner;

public final class StateBuilder {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final ProgramContext programContext;

    // ---------------------------------------------------

    private Scope scope;

    private String at;

    private long rows;

    private long cols;

    private MatrixFormat matrixFormat;

    private FileFormat fileFormat;

    private String path;

    private String labelState;

    private Class<? extends MatrixPartitioner> partitioner;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public StateBuilder(final ProgramContext programContext) {

        this.programContext = Preconditions.checkNotNull(programContext);

        clear();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public StateBuilder scope(Scope scope) { this.scope = scope; return this; }

    public StateBuilder at(String at) { this.at = at; return this; }

    public StateBuilder partitioner(Class<? extends MatrixPartitioner> partitioner) { this.partitioner = partitioner; return this; }

    public StateBuilder rows(long rows) { this.rows = rows; return this; }

    public StateBuilder cols(long cols) { this.cols = cols; return this; }

    public StateBuilder matrixFormat(MatrixFormat matrixFormat) { this.matrixFormat = matrixFormat; return this; }

    public StateBuilder fileFormat(FileFormat fileFormat) { this.fileFormat = fileFormat; return this; }

    public StateBuilder path(String path) { this.path = path; return this; }

    public StateBuilder label(String labelState) { this.labelState = labelState; return this; }

    // ---------------------------------------------------

    public MatrixBase build(final String stateName) throws Exception {
        final StateDescriptor descriptor = new StateDescriptor(
                stateName,
                MatrixBase.class,
                scope,
                ParseUtils.parseNodeRanges(at),
                partitioner,
                rows, cols,
                matrixFormat,
                fileFormat,
                path,
                labelState
        );
        //programContext.runtimeContext.runtimeManager.allocateState(programContext, descriptor);
        return programContext.runtimeContext.runtimeManager.getDHT(stateName);
    }

    // ---------------------------------------------------

    public void clear() {
        this.scope = Scope.REPLICATED;
        this.at = "";
        this.partitioner = RowPartitioner.class;
        this.rows = 0;
        this.cols = 0;
        this.fileFormat = FileFormat.DENSE_FORMAT;
        //this.recordFormat = RowColValRecordIteratorProducer.class;
        this.path = "";
    }
}
