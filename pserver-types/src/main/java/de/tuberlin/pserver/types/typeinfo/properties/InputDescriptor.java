package de.tuberlin.pserver.types.typeinfo.properties;


import de.tuberlin.pserver.types.typeinfo.annotations.Load;

public final class InputDescriptor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private FileFormat fileFormat;

    private String filePath;

    private String labels;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public InputDescriptor(FileFormat fileFormat, String filePath, String labels) {
        this.fileFormat = fileFormat;
        this.filePath = filePath;
        this.labels = labels;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public FileFormat fileFormat() { return fileFormat; }

    public String filePath() { return filePath; }

    public String labels() { return labels; }

    // ---------------------------------------------------
    // Factory Methods.
    // ---------------------------------------------------

    public static InputDescriptor createInputDescriptor(Load an) {
        return new InputDescriptor(an.fileFormat(), an.filePath(), an.labels());
    }
}
