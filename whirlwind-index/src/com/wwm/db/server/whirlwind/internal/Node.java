package com.wwm.db.server.whirlwind.internal;

import java.io.PrintStream;
import java.io.Serializable;
import java.lang.ref.WeakReference;

import org.fuzzydb.attrs.internal.Attribute;
import org.fuzzydb.attrs.internal.ContainsNotSpecifiedAttrConstraint;
import org.fuzzydb.attrs.internal.IConstraintMap;
import org.fuzzydb.client.Ref;
import org.fuzzydb.client.internal.RefImpl;
import org.fuzzydb.core.whirlwind.internal.IAttributeConstraint;
import org.fuzzydb.server.internal.whirlwind.Immutable;
import org.fuzzydb.server.internal.whirlwind.RefAware;



/**
 * FIXME: Extract out a superclass called "ReadOnlyNode" (or something that gives us that control)
 * TODO: Review tree operations on wrt Visitor pattern
 * - see http://developers.sun.com/learning/javaoneonline/2008/pdf/TS-5186.pdf
 */
public abstract class Node<T extends Node<T> & RefAware<T>> implements RefAware<T>, Serializable, Immutable {
	// TODO: RefAware<T> is actually intended to be Node<ConcreteSubClass>, but that's not built in.

	private static final long serialVersionUID = 1L;

	/**
	 * @directed true
	 * @link association Node
	 * @supplierCardinality 0..1
	 */
	private final Ref<BranchNode> parentRef;

	private transient WeakReference<BranchNode> cachedParent = null;
	private transient RefImpl<T> ref;

	/**
	 * Debug support for checking we're not modifying the Node we got out of the
	 */
	protected transient boolean mutable = false;

	/**
	 * Set to true if we want to create a new version (i.e. to indicate we modified
	 * one or more attributes in a clone.
	 * Some actions such as insert and expandAnnotations may check the node, and
	 * then find they don't need to modify it.
	 */
	protected transient boolean modified = false;

	/*=============== CONSTRUCTORS ================*/
	/**
	 * @see java.lang.Object#Object()
	 */
	protected Node(Ref<BranchNode> parent) {
		super();
		this.parentRef = parent;
		this.mutable = true; // just created it, so it's mutable.
	}

	/**
	 * Copy constructor.  Set mutable at this point.
	 * @param rhs
	 */
	public Node(Node<T> rhs) {
		this.parentRef = rhs.parentRef;
		this.ref = rhs.ref;
		this.mutable = true;
	}

	protected Node() {	// used for root node
		parentRef = null;
		this.mutable = true;
	}


	/* (non-Javadoc)
	 * @see com.wwm.db.internal.whirlwind.Immutable#setImmutable()
	 */
	@Override
	public void setImmutable() {
		this.mutable = false;
		this.modified = false;
	}

	public boolean isModified() {
		return modified;
	}

	/**
	 * Indicate that this node has been modified.
	 * NOTE: Use sparsely.  Only use where a real change happens to the node.  Not all code paths modify.
	 * finalise() will tell you if you missed one.
	 */
    protected void setModified() {
		modified = true;
	}


	@Override
	public void setRef(Ref<T> ref) {
		this.ref = (RefImpl<T>) ref;
	}

	@Override
	public Ref<T> getRef() {
		assert( ref != null);
		return ref;
	}



	/* (non-Javadoc)
	 * @see com.wwm.db.internal.whirlwind.NodeR#dumpNode(java.io.PrintStream, int, com.wwm.db.internal.whirlwind.NodeStorageManager)
	 */
	public abstract void dumpNode( PrintStream out, int i, NodeStorageManager index);


	//public abstract void rebuildAttributeCache(AttributeCache attributeCache, NodeStorageManager index);

	/* (non-Javadoc)
	 * @see com.wwm.db.internal.whirlwind.NodeR#getParent(com.wwm.db.internal.whirlwind.NodeStorageManager)
	 */
	public BranchNode getParent(NodeStorageManager index) {
		if (cachedParent != null) {
			return cachedParent.get();
		}

		BranchNode newParent = index.getBranchNode(parentRef);
		cachedParent = new WeakReference<BranchNode>(newParent);
		return cachedParent.get();
	}

	/* (non-Javadoc)
	 * @see com.wwm.db.internal.whirlwind.NodeR#getParentRef()
	 */
	public Ref<BranchNode> getParentRef() {
		return parentRef;
	}

	/* (non-Javadoc)
	 * @see com.wwm.db.internal.whirlwind.NodeR#toString()
	 */
	@Override
	public String toString() {
		return ref.toString();
	}

	/**
	 * Expand the annotations for the given branch
	 * @param constraints
	 * @param att
	 * @param storage
	 * @return
	 */
	protected IAttributeConstraint expandAnnotations(IConstraintMap constraints, Attribute<?> att, NodeStorageManager storage) {
        assert(mutable);

		if (constraints.expand(att, att.getAttrId())) {
			setModified();
			if (parentRef != null) {
				return storage.getWriteableBranch(parentRef).expandAnnotation(att, att.getAttrId(), this.getRef(), storage);
			} else {
				return constraints.findAttr(att.getAttrId()); // at root
			}
		} else {  // have NA, has value didn't expand it (so no need to bubble up)
			return constraints.findAttr(att.getAttrId());
		}
	}


	// FIXME: Consider whether we can avoid generating a writable version of parent
	protected IAttributeConstraint expandNullAnnotations(IConstraintMap constraints, int attId, NodeStorageManager storage) {
        assert(mutable);
        IAttributeConstraint na = constraints.findAttr(attId);
		if (na != null) {
			if (na.isIncludesNotSpecified()) return na;

			na.setIncludesNotSpecified(true);
			setModified(); // We've modified this node

			if (parentRef != null) {
				return storage.getWriteableBranch(parentRef).expandAnnotation(null, attId, this.getRef(), storage);
			} else {
				return na;
			}
		} else {
			na = new ContainsNotSpecifiedAttrConstraint(attId);
			constraints.putAttr(na);
			setModified(); // We've modified this node
			if (parentRef != null) {
				return storage.getWriteableBranch(parentRef).expandAnnotation(null, attId, this.getRef(), storage);
			} else {
				return na;
			}
		}
	}

	@Override
	protected void finalize() {
		assert(!modified); // If object is modified, then we forgot to create or update it after we cloned it to modify it.
	}

	/**
	 * Accept a visitor to this node.  See Visitor pattern (GoF '95)
	 */
	abstract public void accept(NodeVisitor visitor, NodeStorageManager storage);

}
