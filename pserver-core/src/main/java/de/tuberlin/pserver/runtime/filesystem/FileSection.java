package de.tuberlin.pserver.runtime.filesystem;

import com.google.gson.Gson;
import de.tuberlin.pserver.commons.json.GsonUtils;


public class FileSection {

    private transient Gson gson = GsonUtils.createPrettyPrintAndAnnotationExclusionGson();

    public long totalLines  = 0;

    public long linesToRead = 0;

    public long startOffset = 0;

    public long blockLineOffset = 0;

    public void set(long totalLines, long linesToRead, long startOffset, long blockLineOffset) {

        this.totalLines = totalLines;

        this.linesToRead = linesToRead;

        this.startOffset = startOffset;

        this.blockLineOffset = blockLineOffset;
    }

    @Override public String toString() { return "\nFileSection " + gson.toJson(this); }
}
