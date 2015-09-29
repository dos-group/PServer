package de.tuberlin.pserver.dsl.state.annotations;

import de.tuberlin.pserver.dsl.state.properties.GlobalScope;
import de.tuberlin.pserver.dsl.state.properties.LocalScope;
import de.tuberlin.pserver.dsl.state.properties.RemoteUpdate;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.runtime.filesystem.record.config.AbstractRecordFormatConfig;
import de.tuberlin.pserver.runtime.filesystem.record.config.RowColValRecordFormatConfig;
import de.tuberlin.pserver.runtime.partitioning.IMatrixPartitioner;
import de.tuberlin.pserver.runtime.partitioning.MatrixByRowPartitioner;
import de.tuberlin.pserver.types.PartitionType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface State {;

    LocalScope localScope() default LocalScope.SHARED; // NO SUPPORT FOR LOCAL SCOPES AT THE MOMENT....

    GlobalScope globalScope() default GlobalScope.REPLICATED;

    String at() default "";

    Class<? extends IMatrixPartitioner> partitionerClass() default MatrixByRowPartitioner.class;

    long rows() default 0;

    long cols() default 0;

    Layout layout() default Layout.ROW_LAYOUT;

    Format format() default Format.DENSE_FORMAT;

    Class<? extends AbstractRecordFormatConfig> recordFormat() default RowColValRecordFormatConfig.class;

    String path() default "";

    RemoteUpdate remoteUpdate() default RemoteUpdate.NO_UPDATE;
}