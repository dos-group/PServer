package de.tuberlin.pserver.dsl.state;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import de.tuberlin.pserver.commons.utils.ParseUtils;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.GlobalScope;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.runtime.filesystem.record.config.AbstractRecordFormatConfig;
import de.tuberlin.pserver.runtime.partitioning.IMatrixPartitioner;
import de.tuberlin.pserver.types.PartitionType;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.IntStream;

public final class StateDeclaration {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final String name;

    public final Class<?>  stateType;

    public final GlobalScope globalScope;

    public final int[] atNodes;

    public final Class<? extends IMatrixPartitioner> partitionerClass;

    public final long rows;

    public final long cols;

    public final Layout layout;

    public final Format format;

    public final Class<? extends AbstractRecordFormatConfig> recordFormatConfigClass;

    public final String path;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public StateDeclaration(final String name,
                            final Class<?> stateType,
                            final GlobalScope globalScope,
                            final int[] atNodes,
                            final Class<? extends IMatrixPartitioner> partitionerClass,
                            final long rows,
                            final long cols,
                            final Layout layout,
                            final Format format,
                            final Class<? extends AbstractRecordFormatConfig> recordFormatConfigClass,
                            final String path) {

        this.name           = name;
        this.stateType      = stateType;
        this.globalScope    = globalScope;
        this.atNodes        = atNodes;
        this.partitionerClass  = partitionerClass;
        this.rows           = rows;
        this.cols           = cols;
        this.layout         = layout;
        this.format         = format;
        this.recordFormatConfigClass = recordFormatConfigClass;
        this.path           = path;
    }

    public static StateDeclaration fromAnnotatedField(State state, Field field, int[] fallBackAtNodes) {
        int[] parsedAtNodes = ParseUtils.parseNodeRanges(state.at());
        return new StateDeclaration(
                field.getName(),
                field.getType(),
                state.globalScope(),
                parsedAtNodes.length > 0 ? parsedAtNodes : fallBackAtNodes,
                state.partitionerClass(),
                state.rows(),
                state.cols(),
                state.layout(),
                state.format(),
                state.recordFormat(),
                state.path()
        );
    }

}
