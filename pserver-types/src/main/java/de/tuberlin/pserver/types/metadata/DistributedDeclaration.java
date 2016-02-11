package de.tuberlin.pserver.types.metadata;

public abstract class DistributedDeclaration {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final int[] nodes;

    public final DistScheme distScheme;

    public final String name;

    public final Class<?> type;

    // ---------------------------------------------------

    // TODO: Encapsulate that!

    public final FileFormat format;

    public final String path;

    public final String labels;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedDeclaration(int[] nodes, DistScheme distScheme, Class<?> type, String name) {
        this(nodes, distScheme, type, name, FileFormat.UNDEFINED, null, null);
    }

    public DistributedDeclaration(int[] nodes, DistScheme distScheme, Class<?> type, String name, FileFormat format, String path, String labels) {
        this.nodes          = nodes;
        this.distScheme     = distScheme;
        this.type           = type;
        this.name           = name;
        this.format         = format;
        this.path           = path;
        this.labels         = labels;
    }
}
