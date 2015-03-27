package de.tuberlin.pserver.core.filesystem;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.core.filesystem.hdfs.FileInputSplit;
import de.tuberlin.pserver.core.filesystem.hdfs.in.CSVInputFormat;
import de.tuberlin.pserver.core.filesystem.hdfs.in.InputFormat;
import de.tuberlin.pserver.math.experimental.tuples.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class FileDataIterator implements Iterator<Tuple> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(FileDataIterator.class);

    private final DataManager dataManager;

    private final InputFormat<Tuple, FileInputSplit> inputFormat;

    private FileInputSplit split;

    private Tuple record;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public FileDataIterator(final String filePath,
                            final Class<?>[] fieldTypes,
                            final DataManager dataManager) {

        this.dataManager = Preconditions.checkNotNull(dataManager);

        inputFormat = new CSVInputFormat(new Path(Preconditions.checkNotNull(filePath)), fieldTypes);

        final Configuration conf = new Configuration();

        //conf.set("fs.defaultFS", dataManager.getProperty());

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
