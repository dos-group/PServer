package de.tuberlin.pserver.dsl.state;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.utils.ParseUtils;
import de.tuberlin.pserver.compiler.StateDescriptor;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.ProgramContext;
import de.tuberlin.pserver.runtime.filesystem.record.IRecordIteratorProducer;
import de.tuberlin.pserver.runtime.filesystem.record.RowColValRecordIteratorProducer;
import de.tuberlin.pserver.runtime.partitioning.partitioner.IMatrixPartitioner;
import de.tuberlin.pserver.runtime.partitioning.partitioner.RowPartitioner;

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

    private Layout layout;

    private Format format;

    private String path;

    private Class<? extends IRecordIteratorProducer> recordFormat;

    private Class<? extends IMatrixPartitioner> partitioner;

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

    public StateBuilder partitioner(Class<? extends IMatrixPartitioner> partitioner) { this.partitioner = partitioner; return this; }

    public StateBuilder rows(long rows) { this.rows = rows; return this; }

    public StateBuilder cols(long cols) { this.cols = cols; return this; }

    public StateBuilder layout(Layout layout) { this.layout = layout; return this; }

    public StateBuilder format(Format format) { this.format = format; return this; }

    public StateBuilder recordFormat(Class<? extends IRecordIteratorProducer> recordFormat) { this.recordFormat = recordFormat; return this; }

    public StateBuilder path(String path) { this.path = path; return this; }

    // ---------------------------------------------------

    public Matrix build(final String stateName) throws Exception {
        final StateDescriptor descriptor = new StateDescriptor(
                stateName,
                Matrix.class,
                scope,
                ParseUtils.parseNodeRanges(at),
                partitioner,
                rows, cols,
                layout,
                format,
                recordFormat,
                path
        );
        programContext.runtimeContext.runtimeManager.allocateState(programContext, descriptor);
        return programContext.runtimeContext.runtimeManager.getDHT(stateName);
    }

    // ---------------------------------------------------

    public void clear() {
        this.scope = Scope.REPLICATED;
        this.at = "";
        this.partitioner = RowPartitioner.class;
        this.rows = 0;
        this.cols = 0;
        this.layout = Layout.ROW_LAYOUT;
        this.format = Format.DENSE_FORMAT;
        this.recordFormat = RowColValRecordIteratorProducer.class;
        this.path = "";
    }
}
