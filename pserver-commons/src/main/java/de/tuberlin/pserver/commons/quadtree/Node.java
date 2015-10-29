package de.tuberlin.pserver.commons.quadtree;

public class Node {

    private double x;
    private double y;
    private double w;
    private double h;
    private int numPoints;
    private Node opt_parent;
    private Point point;
    private Point centerOfMass;
    private Point sum;
    private NodeType nodetype = NodeType.EMPTY;
    private Node nw;
    private Node ne;
    private Node sw;
    private Node se;

    /**
     * Constructs a new quad tree node.
     *
     * @param x X-coordiate of node.
     * @param y Y-coordinate of node.
     * @param w Width of node.
     * @param h Height of node.
     * @param opt_parent Optional parent node.
     */
    public Node(double x, double y, double w, double h, Node opt_parent) {
        this.x = x;
        this.y = y;
        this.w = w;
        this.h = h;
        this.opt_parent = opt_parent;
        this.numPoints = 0;
        this.centerOfMass = new Point(0, 0);
        this.sum = new Point(0, 0);
    }

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    public double getW() {
        return w;
    }

    public void setW(double w) {
        this.w = w;
    }

    public double getH() {
        return h;
    }

    public void setH(double h) {
        this.h = h;
    }

    public Node getParent() {
        return opt_parent;
    }

    public void setParent(Node opt_parent) {
        this.opt_parent = opt_parent;
    }

    public void setPoint(Point point) {
        this.point = point;
    }

    public Point getPoint() {
        return this.point;
    }

    public void setNodeType(NodeType nodetype) {
        this.nodetype = nodetype;
    }

    public NodeType getNodeType() {
        return this.nodetype;
    }

    public void setNw(Node nw) {
        this.nw = nw;
    }

    public void setNe(Node ne) {
        this.ne = ne;
    }

    public void setSw(Node sw) {
        this.sw = sw;
    }

    public void setSe(Node se) {
        this.se = se;
    }

    public Node getNe() {
        return ne;
    }

    public Node getNw() {
        return nw;
    }

    public Node getSw() {
        return sw;
    }

    public Node getSe() {
        return se;
    }

    public void addPoint(Point point) {
        this.numPoints += 1;
        this.sum = new Point(this.sum.getX() + point.getX(), this.sum.getY() + point.getY());
        this.centerOfMass = new Point(this.sum.getX() / this.numPoints,
                this.sum.getY() / this.numPoints);
    }

    public Point getCenterOfMass() {
        return this.centerOfMass;
    }

    public int getNumPoints() {
        return this.numPoints;
    }
}