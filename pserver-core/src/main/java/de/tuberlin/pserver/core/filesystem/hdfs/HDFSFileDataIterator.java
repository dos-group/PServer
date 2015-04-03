package de.tuberlin.pserver.core.filesystem.hdfs;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.filesystem.FileDataIterator;
import de.tuberlin.pserver.core.filesystem.hdfs.in.CSVInputFormat;
import de.tuberlin.pserver.core.filesystem.hdfs.in.InputFormat;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.math.experimental.tuples.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HDFSFileDataIterator implements FileDataIterator<Tuple> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(HDFSFileDataIterator.class);

    private final MachineDescriptor machine;

    private final InputSplitProvider inputSplitProvider;

    private final InputFormat<Tuple, FileInputSplit> inputFormat;

    private FileInputSplit split;

    private Tuple record;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    @SuppressWarnings("unchecked")
    public HDFSFileDataIterator(final IConfig config,
                                final MachineDescriptor machine,
                                final InputSplitProvider inputSplitProvider,
                                final String filePath,
                                final Class<?>[] fieldTypes) {

        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(filePath);
        Preconditions.checkNotNull(fieldTypes);

        this.machine = Preconditions.checkNotNull(machine);
        this.inputSplitProvider = Preconditions.checkNotNull(inputSplitProvider);
        this.inputFormat = new CSVInputFormat(new Path(filePath), fieldTypes);

        final Configuration conf = new Configuration();
        conf.set("fs.defaultFS", config.getString("filesystem.hdfs.url"));
        this.inputFormat.configure(conf);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void initialize() {}

    @Override
    public boolean hasNext() { return false; } // FIXME: ...implementation...

    @Override
    public Tuple next() {
        try {
            inputFormat.nextRecord(record);
            if (inputFormat.reachedEnd()) {
                inputFormat.close();
                split = (FileInputSplit)inputSplitProvider.getNextInputSplit(machine);
                if (split == null)
                    return null;
                inputFormat.open(split);
                inputFormat.nextRecord(record);
            }
            return record;
        } catch(IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
