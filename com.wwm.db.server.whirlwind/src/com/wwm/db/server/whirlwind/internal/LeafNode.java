package com.wwm.db.server.whirlwind.internal;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;

import org.fuzzydb.attrs.AttributeMapFactory;
import org.fuzzydb.attrs.internal.Attribute;
import org.fuzzydb.attrs.internal.IConstraintMap;
import org.fuzzydb.client.Ref;
import org.fuzzydb.client.marker.IWhirlwindItem;
import org.fuzzydb.core.Settings;
import org.fuzzydb.core.marker.IAttributeContainer;
import org.fuzzydb.core.whirlwind.internal.IAttribute;
import org.fuzzydb.core.whirlwind.internal.IAttributeConstraint;



/**
 *
 */
public class LeafNode extends Node<LeafNode> {

	private static final long serialVersionUID = 3256725061254658100L;
    private static final int CRITICAL_MASS = Settings.getInstance().getLeafCriticalMass();

	ArrayList<IWhirlwindItem> items = new ArrayList<IWhirlwindItem>(); // Was objectVersion

	/**
	 * Bounds of content of leafNode
	 */
	private IConstraintMap bubble = AttributeMapFactory.newConstraintMap();


	public LeafNode(Ref<BranchNode> parent) {
		super(parent);
	}

    public LeafNode(LeafNode clonee) {
		super(clonee);
		this.items = new ArrayList<IWhirlwindItem>(clonee.items);
		this.bubble = clonee.bubble.clone();
	}

	/**
     * @return items
	 */
    public Collection<IWhirlwindItem> getItems() {
    	return items;
    }

    /**
     * Add item to this leaf's array.
     * @param item DbTreeItem to add.
     */
    private final void add( IWhirlwindItem item ) {
    	assert( items.size() <= CRITICAL_MASS);
    	setModified();

    	items.add(item);
    }


	/*
     * Annotation expansion algo
     *
     * 1. Expand leaf node's bubble.
     * 2. If the bubble does not expand, end.
     * 3. Move to parent node.
     * 4. If current node has annotation, expand.
     *  4b. If bubble did not expand, end
     * 5. If current node does not have annotation, create annotation
     * 6. If current node is root, end
     * 7. otherwise go to step 3.
     *
     * Optimisation algo
     *
     * If a branch has an annotation equal to the annotaion on it's first immediate ancestor with the same type annotation,
     * remove the annotation from this branch.
     *
     */


	@Override
	public void accept(NodeVisitor visitor, NodeStorageManager storage) {
		visitor.visit(this, storage);
	}


	/**
	 * FIXME:  ALERT ALERT!!
	 * NOTE/THOUGHT (from Neale): I'm guessing that this worked before because we'd done a getObject(RefImpl) from the table, and therefore
	 * it could only be the one object instance in memory.  That was true before, but isn't true now
	 */
	boolean contains(IAttributeContainer item) {
		for (IAttributeContainer ov : items) {
			if (ov == item) return true;
		}
		return false;
	}

    void safeInsert(IWhirlwindItem inserted, NodeStorageManager storage) {
    	assert(mutable);
    	//BranchNode parent = null;
		add(inserted);

		// Expand bubble with all attributes and send them up the tree
		for (IAttribute ia : inserted.getAttributeMap()) {
			// if the annotation isn't already in the bubble, and this node is not empty, all previous items didn't
			// specify the attribute, so bubble up a null
			if (items.size() > 1) {	// size is 1 here if this is the first insert, >1 otherwise
				// this is not the first item
				if (bubble.findAttr(ia.getAttrId()) == null) {
					// the att id is not in the bubble. So all previous inserts must be missing this attribute.
					expandNullAnnotations(bubble, ia.getAttrId(), storage);
				}
			}

			Attribute<?> att = (Attribute<?>)ia;
			expandAnnotations(bubble, att, storage);
		}

		// for every attribute in bubble, check for null and bubble it up
		for (IAttribute ia : bubble) { // This could return IAttributeConstraint iterator, methinks ..?
			IAttributeConstraint na = (IAttributeConstraint)ia;
			IAttribute supplied = inserted.getAttributeMap().findAttr(na.getAttrId());
			if (supplied == null && !na.isIncludesNotSpecified()) {
				expandNullAnnotations(bubble, na.getAttrId(), storage);
			}
		}

		// Check for critical mass and split node if necessary.
		if (items.size() > CRITICAL_MASS ) {
		    accept(new LeafSplitVisitor(), storage);
		}
    }


    /** Display count of children + attributeContainer */
	public String toString(NodeStorageManager storage) {
        StringBuffer buff = new StringBuffer();
        buff.append( "num items: " + items.size() + System.getProperty("line.separator"));
        //buff.append( getAttributes().toString() );
        return buff.toString();
    }


    /* (non-Javadoc)
     * @see com.wwm.db.internal.whirlwind.Node#dumpNode(java.io.PrintStream)
     */
    @Override
	public void dumpNode(PrintStream out, int depth, NodeStorageManager storage) {

        out.println( "[" + depth + "] === Leaf ===" );
        out.println( this.toString() );
    }


    @Override
    public String toString() {
    	return super.toString() + ": numItems = " + items.size()
    		+ "\n bounds = " + bubble.toString();
    }
}
