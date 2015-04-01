package de.tuberlin.pserver.core.filesystem.hdfs;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.core.filesystem.FileDataIterator;
import de.tuberlin.pserver.core.filesystem.hdfs.in.CSVInputFormat;
import de.tuberlin.pserver.core.filesystem.hdfs.in.InputFormat;
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

    private final DataManager dataManager;

    private final InputFormat<Tuple, FileInputSplit> inputFormat;

    private FileInputSplit split;

    private Tuple record;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public HDFSFileDataIterator(final DataManager dataManager,
                                final String filePath,
                                final Class<?>[] fieldTypes) {

        this.dataManager = Preconditions.checkNotNull(dataManager);

        inputFormat = new CSVInputFormat(new Path(Preconditions.checkNotNull(filePath)), fieldTypes);

        final Configuration conf = new Configuration();

        conf.set("fs.defaultFS", dataManager.getConfig().getString("filesystem.hdfs.url"));

        inputFormat.configure(conf);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Tuple next() {
        try {
            inputFormat.nextRecord(record);
            if (inputFormat.reachedEnd()) {
                inputFormat.close();
                split = (FileInputSplit)dataManager.getNextInputSplit();
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
