package de.tuberlin.pserver.commons.quadtree;

import de.tuberlin.pserver.commons.tuples.Tuple2;

import java.util.ArrayList;
import java.util.List;


/**
 * Datastructure: A point Quad Tree for representing 2D data. Each
 * region has the same ratio as the bounds for the tree.
 * <p/>
 * The implementation currently requires pre-determined bounds for data as it
 * can not rebalance itself to that degree.
 */
public class QuadTree {


    private Node root_;
    private int count_ = 0;

    /**
     * Constructs a new quad tree.
     *
     * @param minX Minimum x-value that can be held in tree.
     * @param minY Minimum y-value that can be held in tree.
     * @param maxX Maximum x-value that can be held in tree.
     * @param maxY Maximum y-value that can be held in tree.
     */
    public QuadTree(double minX, double minY, double maxX, double maxY) {
        this.root_ = new Node(minX, minY, maxX - minX, maxY - minY, null);
    }

    /**
     * Returns a reference to the tree's root node.  Callers shouldn't modify nodes,
     * directly.  This is a convenience for visualization and debugging purposes.
     *
     * @return {Node} The root node.
     */
    public Node getRootNode() {
        return this.root_;
    }

    /**
     * Sets the value of an (x, y) point within the quad-tree.
     *
     * @param x The x-coordinate.
     * @param y The y-coordinate.
     * @param value The value associated with the point.
     */
    public void set(double x, double y, Object value) {

        Node root = this.root_;
        if (x < root.getX() || y < root.getY() || x > root.getX() + root.getW() || y > root.getY() + root.getH()) {
            throw new QuadTreeException("Out of bounds : (" + x + ", " + y + ")");
        }
        if (this.insert(root, new Point(x, y, value))) {
            this.count_++;
        }
    }

    public void set(double x, double y) {
        this.set(x, y, null);
    }

    /**
     * Gets the value of the point at (x, y) or null if the point is empty.
     *
     * @param x The x-coordinate.
     * @param y The y-coordinate.
     * @param opt_default The default value to return if the node doesn't exist.
     * @return {*} The value of the node, the default value if the node
     *         doesn't exist, or undefined if the node doesn't exist and no default
     *         has been provided.
     */
    public Object get(double x, double y, Object opt_default) {
        Node node = this.find(this.root_, x, y);
        return node != null ? node.getPoint().getValue() : opt_default;
    }

    /**
     * Removes a point from (x, y) if it exists.
     *
     * @param x The x-coordinate.
     * @param y The y-coordinate.
     * @return The value of the node that was removed, or null if the
     *         node doesn't exist.
     */
    public Object remove(double x, double y) {
        Node node = this.find(this.root_, x, y);
        if (node != null) {
            Object value = node.getPoint().getValue();
            node.setPoint(null);
            node.setNodeType(NodeType.EMPTY);
            this.balance(node);
            this.count_--;
            return value;
        } else {
            return null;
        }
    }

    /**
     * Returns true if the point at (x, y) exists in the tree.
     *
     * @param x The x-coordinate.
     * @param y The y-coordinate.
     * @return Whether the tree contains a point at (x, y).
     */
    public boolean contains(double x, double y) {
        return this.get(x, y, null) != null;
    }

    /**
     * @return {boolean} Whether the tree is empty.
     */
    public boolean isEmpty() {
        return this.root_.getNodeType() == NodeType.EMPTY;
    }

    /**
     * @return {number} The number of items in the tree.
     */
    public int getCount() {
        return this.count_;
    }

    /**
     * Removes all items from the tree.
     */
    public void clear() {
        this.root_.setNw(null);
        this.root_.setNe(null);
        this.root_.setSw(null);
        this.root_.setSe(null);
        this.root_.setNodeType(NodeType.EMPTY);
        this.root_.setPoint(null);
        this.count_ = 0;
    }

    /**
     * Returns an array containing the coordinates of each point stored in the tree.
     * @return Array of coordinates.
     */
    public Point[] getKeys() {
        final List<Point> arr = new ArrayList<Point>();
        this.traverse(this.root_, new Func() {
            @Override
            public void call(QuadTree quadTree, Node node) {
                arr.add(node.getPoint());
            }
        });
        return arr.toArray(new Point[arr.size()]);
    }

    /**
     * Returns an array containing all values stored within the tree.
     * @return The values stored within the tree.
     */
    public Object[] getValues() {
        final List<Object> arr = new ArrayList<Object>();
        this.traverse(this.root_, new Func() {
            @Override
            public void call(QuadTree quadTree, Node node) {
                arr.add(node.getPoint().getValue());
            }
        });

        return arr.toArray(new Object[arr.size()]);
    }

    public Point[] searchIntersect(final double xmin, final double ymin, final double xmax, final double ymax) {
        final List<Point> arr = new ArrayList<Point>();
        this.navigate(this.root_, new Func() {
            @Override
            public void call(QuadTree quadTree, Node node) {
                Point pt = node.getPoint();
                if (pt.getX() < xmin || pt.getX() > xmax || pt.getY() < ymin || pt.getY() > ymax) {
                    // Definitely not within the polygon!
                } else {
                    arr.add(node.getPoint());
                }

            }
        }, xmin, ymin, xmax, ymax);
        return arr.toArray(new Point[arr.size()]);
    }

    public Point[] searchWithin(final double xmin, final double ymin, final double xmax, final double ymax) {
        final List<Point> arr = new ArrayList<Point>();
        this.navigate(this.root_, new Func() {
            @Override
            public void call(QuadTree quadTree, Node node) {
                Point pt = node.getPoint();
                if (pt.getX() > xmin && pt.getX() < xmax && pt.getY() > ymin && pt.getY() < ymax) {
                    arr.add(node.getPoint());
                }
            }
        }, xmin, ymin, xmax, ymax);
        return arr.toArray(new Point[arr.size()]);
    }

    public void navigate(Node node, Func func, double xmin, double ymin, double xmax, double ymax) {
        switch (node.getNodeType()) {
            case LEAF:
                func.call(this, node);
                break;

            case POINTER:
                if (intersects(xmin, ymax, xmax, ymin, node.getNe()))
                    this.navigate(node.getNe(), func, xmin, ymin, xmax, ymax);
                if (intersects(xmin, ymax, xmax, ymin, node.getSe()))
                    this.navigate(node.getSe(), func, xmin, ymin, xmax, ymax);
                if (intersects(xmin, ymax, xmax, ymin, node.getSw()))
                    this.navigate(node.getSw(), func, xmin, ymin, xmax, ymax);
                if (intersects(xmin, ymax, xmax, ymin, node.getNw()))
                    this.navigate(node.getNw(), func, xmin, ymin, xmax, ymax);
                break;
        }
    }

    private boolean intersects(double left, double bottom, double right, double top, Node node) {
        return !(node.getX() > right ||
                (node.getX() + node.getW()) < left ||
                node.getY() > bottom ||
                (node.getY() + node.getH()) < top);
    }
    /**
     * Clones the quad-tree and returns the new instance.
     * @return A clone of the tree.
     */
    public QuadTree clone() {
        double x1 = this.root_.getX();
        double y1 = this.root_.getY();
        double x2 = x1 + this.root_.getW();
        double y2 = y1 + this.root_.getH();
        final QuadTree clone = new QuadTree(x1, y1, x2, y2);
        // This is inefficient as the clone needs to recalculate the structure of the
        // tree, even though we know it already.  But this is easier and can be
        // optimized when/if needed.
        this.traverse(this.root_, new Func() {
            @Override
            public void call(QuadTree quadTree, Node node) {
                clone.set(node.getPoint().getX(), node.getPoint().getY(), node.getPoint().getValue());
            }
        });


        return clone;
    }

    /**
     * Traverses the tree depth-first, with quadrants being traversed in clockwise
     * order (NE, SE, SW, NW).  The provided function will be called for each
     * leaf node that is encountered.
     * @param node The current node.
     * @param func The function to call
     *     for each leaf node. This function takes the node as an argument, and its
     *     return value is irrelevant.
     */
    public void traverse(Node node, Func func) {
        switch (node.getNodeType()) {
            case LEAF:
                func.call(this, node);
                break;

            case POINTER:
                this.traverse(node.getNe(), func);
                this.traverse(node.getSe(), func);
                this.traverse(node.getSw(), func);
                this.traverse(node.getNw(), func);
                break;
        }
    }

    /**
     * Finds a leaf node with the same (x, y) coordinates as the target point, or
     * null if no point exists.
     * @param node The node to search in.
     * @param x The x-coordinate of the point to search for.
     * @param y The y-coordinate of the point to search for.
     * @return The leaf node that matches the target,
     *     or null if it doesn't exist.
     */
    public Node find(Node node, double x, double y) {
        Node resposne = null;
        switch (node.getNodeType()) {
            case EMPTY:
                break;

            case LEAF:
                resposne = node.getPoint().getX() == x && node.getPoint().getY() == y ? node : null;
                break;

            case POINTER:
                resposne = this.find(this.getQuadrantForPoint(node, x, y), x, y);
                break;

            default:
                throw new QuadTreeException("Invalid nodeType");
        }
        return resposne;
    }

    public Tuple2<Point, Double> computeRepulsiveForce(Node node, Point point, double theta) {
        Tuple2<Point, Double> result = new Tuple2<>(new Point(0, 0), 0.0);

        switch (node.getNodeType()) {
            case LEAF:
                if (node.getPoint().compareTo(point) != 0) {
                    result = computeForce(point, node, theta);
                }
                break;

            case POINTER:
                double distance = squaredDistance(point, node.getCenterOfMass());

                if (node.getW() / distance < theta) {
                    result = computeForce(point, node, theta);
                } else {
                    Tuple2<Point, Double> r1 = this.computeRepulsiveForce(node.getNe(), point, theta);
                    Tuple2<Point, Double> r2 = this.computeRepulsiveForce(node.getSe(), point, theta);
                    Tuple2<Point, Double> r3 = this.computeRepulsiveForce(node.getSw(), point, theta);
                    Tuple2<Point, Double> r4 = this.computeRepulsiveForce(node.getNw(), point, theta);

                    Point combinedForce = new Point(
                            r1._1.getX() + r2._1.getX() + r3._1.getX() + r4._1.getX(),
                            r1._1.getY() + r2._1.getY() + r3._1.getY() + r4._1.getY());

                    double sumQ = r1._2 + r2._2 + r3._2 + r4._2;

                    result = new Tuple2<>(combinedForce, sumQ);
                }
                break;
        }

        return result;
    }

    private Tuple2<Point, Double> computeForce(Point point, Node node, double theta) {
        double distance = squaredDistance(point, node.getCenterOfMass());
        double q = 1.0 / (1.0 + distance);
        double sumQ = 0.0;

        Point distanceVector = new Point(point.getX() - node.getCenterOfMass().getX(),
                point.getY() - node.getCenterOfMass().getY());

        double mult = node.getNumPoints() * q;
        sumQ += mult;

        Point repForce = new Point(mult * q * distanceVector.getX(), mult * q * distanceVector.getY());
        return new Tuple2<>(repForce, sumQ);
    }

    private double squaredDistance(Point a, Point b) {
        return (Math.pow(a.getX() - b.getX(), 2) + Math.pow(a.getY() - b.getY(), 2));
    }

    /**
     * Inserts a point into the tree, updating the tree's structure if necessary.
     * @param parent The parent to insert the point into.
     * @param point The point to insert.
     * @return True if a new node was added to the tree; False if a node
     *     already existed with the correpsonding coordinates and had its value
     *     reset.
     */
    private boolean insert(Node parent, Point point) {
        Boolean result = false;
        switch (parent.getNodeType()) {
            case EMPTY:
                this.setPointForNode(parent, point);
                result = true;
                parent.addPoint(point);
                break;
            case LEAF:
                if (parent.getPoint().getX() == point.getX() && parent.getPoint().getY() == point.getY()) {
                    this.setPointForNode(parent, point);
                    result = false;
                } else {
                    this.split(parent);
                    result = this.insert(parent, point);
                    if (result) {
                        parent.addPoint(point);
                    }
                }
                break;
            case POINTER:
                result = this.insert(
                        this.getQuadrantForPoint(parent, point.getX(), point.getY()), point);
                if (result) {
                    parent.addPoint(point);
                }
                break;

            default:
                throw new QuadTreeException("Invalid nodeType in parent");
        }
        return result;
    }

    /**
     * Converts a leaf node to a pointer node and reinserts the node's point into
     * the correct child.
     * @param node The node to split.
     */
    private void split(Node node) {
        Point oldPoint = node.getPoint();
        node.setPoint(null);

        node.setNodeType(NodeType.POINTER);

        double x = node.getX();
        double y = node.getY();
        double hw = node.getW() / 2;
        double hh = node.getH() / 2;

        node.setNw(new Node(x, y, hw, hh, node));
        node.setNe(new Node(x + hw, y, hw, hh, node));
        node.setSw(new Node(x, y + hh, hw, hh, node));
        node.setSe(new Node(x + hw, y + hh, hw, hh, node));

        this.insert(node, oldPoint);
    }

    /**
     * Attempts to balance a node. A node will need balancing if all its children
     * are empty or it contains just one leaf.
     * @param node The node to balance.
     */
    private void balance(Node node) {
        switch (node.getNodeType()) {
            case EMPTY:
            case LEAF:
                if (node.getParent() != null) {
                    this.balance(node.getParent());
                }
                break;

            case POINTER: {
                Node nw = node.getNw();
                Node ne = node.getNe();
                Node sw = node.getSw();
                Node se = node.getSe();
                Node firstLeaf = null;

                // Look for the first non-empty child, if there is more than one then we
                // break as this node can't be balanced.
                if (nw.getNodeType() != NodeType.EMPTY) {
                    firstLeaf = nw;
                }
                if (ne.getNodeType() != NodeType.EMPTY) {
                    if (firstLeaf != null) {
                        break;
                    }
                    firstLeaf = ne;
                }
                if (sw.getNodeType() != NodeType.EMPTY) {
                    if (firstLeaf != null) {
                        break;
                    }
                    firstLeaf = sw;
                }
                if (se.getNodeType() != NodeType.EMPTY) {
                    if (firstLeaf != null) {
                        break;
                    }
                    firstLeaf = se;
                }

                if (firstLeaf == null) {
                    // All child nodes are empty: so make this node empty.
                    node.setNodeType(NodeType.EMPTY);
                    node.setNw(null);
                    node.setNe(null);
                    node.setSw(null);
                    node.setSe(null);

                } else if (firstLeaf.getNodeType() == NodeType.POINTER) {
                    // Only child was a pointer, therefore we can't rebalance.
                    break;

                } else {
                    // Only child was a leaf: so update node's point and make it a leaf.
                    node.setNodeType(NodeType.LEAF);
                    node.setNw(null);
                    node.setNe(null);
                    node.setSw(null);
                    node.setSe(null);
                    node.setPoint(firstLeaf.getPoint());
                }

                // Try and balance the parent as well.
                if (node.getParent() != null) {
                    this.balance(node.getParent());
                }
            }
            break;
        }
    }

    /**
     * Returns the child quadrant within a node that contains the given (x, y)
     * coordinate.
     * @param parent The node.
     * @param x The x-coordinate to look for.
     * @param y The y-coordinate to look for.
     * @return The child quadrant that contains the point.
     */
    private Node getQuadrantForPoint(Node parent, double x, double y) {
        double mx = parent.getX() + parent.getW() / 2;
        double my = parent.getY() + parent.getH() / 2;
        if (x < mx) {
            return y < my ? parent.getNw() : parent.getSw();
        } else {
            return y < my ? parent.getNe() : parent.getSe();
        }
    }

    /**
     * Sets the point for a node, as long as the node is a leaf or empty.
     * @param node The node to set the point for.
     * @param point The point to set.
     */
    private void setPointForNode(Node node, Point point) {
        if (node.getNodeType() == NodeType.POINTER) {
            throw new QuadTreeException("Can not set point for node of type POINTER");
        }
        node.setNodeType(NodeType.LEAF);
        node.setPoint(point);
    }
}
