package de.tuberlin.pserver.runtime.filesystem;


import com.google.gson.Gson;
import de.tuberlin.pserver.commons.json.GsonUtils;
import de.tuberlin.pserver.types.typeinfo.properties.FileFormat;

public abstract class AbstractFilePartition {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    //protected transient Gson gson = GsonUtils.createPrettyPrintAndAnnotationExclusionGson();

    public final int nodeID;

    public final String file;

    public final FileFormat fileFormat;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public AbstractFilePartition() { this(-1, null, null); }
    public AbstractFilePartition(int nodeID, String file, FileFormat fileFormat) {
        this.nodeID     = nodeID;
        this.file       = file;
        this.fileFormat = fileFormat;
    }
}
