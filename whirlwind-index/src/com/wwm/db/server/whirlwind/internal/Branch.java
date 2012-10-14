package com.wwm.db.server.whirlwind.internal;

import java.io.PrintStream;
import java.io.Serializable;

import org.fuzzydb.attrs.AttributeMapFactory;
import org.fuzzydb.attrs.internal.Attribute;
import org.fuzzydb.attrs.internal.IConstraintMap;
import org.fuzzydb.client.Ref;
import org.fuzzydb.client.internal.RefImpl;
import org.fuzzydb.core.marker.IAttributeContainer;
import org.fuzzydb.core.whirlwind.internal.IAttributeConstraint;


/**
 * This is one of 3 or more branches from a given node (3 being: higher, lower and not-specified).
 * The BranchConstraint specifies what is allowed in this node. It will match the splitId, as we only
 * split on one attribute at a time, so the parent nodes in the tree will have constrained other attributes
 * to this point.
 *
 * @author Adrian
 * Documentation: Neale
 *
 */
public class Branch implements Serializable {

	private static final long serialVersionUID = 1L;

	private final int splitId;	// The ID we split on, or -1 if not split (root?)

	private final IAttributeConstraint constraint;
	private final IConstraintMap annotations;	// these are the same as parent annotations, so we don't need to score against them
	private Ref<? extends Node<?>> child;

	public Branch(RefImpl<Node<?>> child) {
		this(child, null, -1);
	}

	public Branch(RefImpl<Node<?>> child, IAttributeConstraint constraint, int splitId) {
		this.constraint = constraint;
		annotations = AttributeMapFactory.newConstraintMap();
		this.child = child;
		this.splitId = splitId;
	}

	public Branch(Branch clonee) {
		super();
		splitId = clonee.splitId;
		constraint = clonee.constraint;
		annotations = clonee.annotations.clone();
		child = clonee.child;
	}

	public IConstraintMap getAnnotations() {
		return annotations;
	}


	/** Determine if the specified index IWhirlwindItem is consistent with the branch attributes on this node
	 * @param item
	 * @return true or false
	 */
	protected boolean consistent(IAttributeContainer item) {
		if (splitId == -1) return true;	// root branch or unconstrained branch, no split, everything goes
		Attribute att = (Attribute)item.getAttributeMap().findAttr(splitId);
		if (att == null) {
			if (constraint == null) return true;	// no attribute + no constraint, this is the right branch
			return false; // no attribute but there is a constraint, wrong branch - need the one with no contraint
		}
		if (constraint == null) return false;	// there is an attribute matching split id, must select a constrained branch

		return constraint.consistent(att);
	}



	public Node<?> getChild(NodeStorageManager storage) {
		return storage.getNode(child);
	}

	public Node<?> getWriteableChild(NodeStorageManager storage) {
		return storage.getWriteableNode(child);
	}

	public Ref<? extends Node<?>> getChildRef() {
		return child;
	}

	public void setChild(Node<? extends Node<?>> child) {
		this.child = child.getRef();
	}


	public void dumpNode(PrintStream out, int i, NodeStorageManager storage) {
		getChild(storage).dumpNode(out, i, storage);
	}

//	public void compress(WhirlwindIndex index) {
//		getChild(index).compress(index);
//	}

	@Override
	public String toString() {
		String rval = child.toString() + " Annotations: " + annotations.toString() + System.getProperty( "line.separator");
		return rval;
	}

	public int getSplitId() {
		return splitId;
	}

	public IAttributeConstraint getConstraint() {
		return constraint;
	}
}
