package de.tuberlin.pserver.benchmarks.criteo.logreg;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.commons.config.ConfigLoader;
import de.tuberlin.pserver.types.matrix.annotations.Matrix;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.dense.DenseMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.sparse.CSRMatrix32F;
import de.tuberlin.pserver.types.typeinfo.annotations.Load;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;

import java.io.ByteArrayInputStream;

public class  LoadCriteoData extends Program {

/*
    fs.default.name = "hdfs://"${runtime.hostname}":45010/"
    ./hadoop dfsadmin -report
    ./hadoop fsck /criteo/criteo_train -files -blocks

    Status: HEALTHY
    Total size:	                    102779460059 B
    Total dirs:	                    1
    Total files:	                16
    Total symlinks:		            0
    Total blocks (validated):	    774 (avg. block size 132790000 B)
    Minimally replicated blocks:	774 (100.0 %)
    Over-replicated blocks:	        0 (0.0 %)
    Under-replicated blocks:	    0 (0.0 %)
    Mis-replicated blocks:		    0 (0.0 %)
    Default replication factor:	    3
    Average block replication:	    3.0
    Corrupt blocks:		            0
    Missing replicas:		        0 (0.0 %)
    Number of data-nodes:		    16
    Number of racks:		        1
    FSCK ended at Tue Mar 08 22:38:18 CET 2016 in 29 milliseconds

    systems/flink-0.10.0/bin/flink run -c de.tuberlin.pserver.criteo.CriteoPreprocessingJob pserver-criteo-1.0-SNAPSHOT.jar --input hdfs://wally099.cit.tu-berlin.de:45010/criteo/criteo_small --output hdfs://wally099.cit.tu-berlin.de:45010/criteo/ --mean /home/tobias.herb/mean.csv --stdDeviation /home/tobias.herb/stdDeviation.csv
*/


    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final String DATA_PATH = "/criteo/criteo_train";
    private static final long ROWS = 195841983;
    private static final long COLS = 1048615;

    //private static final String DATA_PATH = "datasets/svm_train";
    //private static final long ROWS = 80000;
    //private static final long COLS = 1048615;

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = ROWS, cols = 1)
    public DenseMatrix32F labels;

    @Load(filePath = DATA_PATH, labels = "labels")
    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = ROWS, cols = COLS)
    public CSRMatrix32F features;

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit
    public void unit(Lifecycle lifecycle) {
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        PServerExecutor.DISTRIBUTED
                .run(ConfigLoader.loadResource("distributed.conf"), LoadCriteoData.class)
                .done();
    }
}
