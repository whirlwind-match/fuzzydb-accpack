package com.wwm.db.server.whirlwind.internal;

import java.io.PrintStream;

import org.fuzzydb.attrs.internal.Attribute;
import org.fuzzydb.attrs.internal.IConstraintMap;
import org.fuzzydb.client.Ref;
import org.fuzzydb.client.internal.RefImpl;
import org.fuzzydb.core.whirlwind.internal.IAttributeConstraint;



/**
 * A BranchNode is a type of Node that contains two or more child nodes, which can be
 * any type of Node (usually derived from BranchNode or LeafNode).
 */
public class BranchNode extends Node<BranchNode> {

    private static final long serialVersionUID = 3256718489887519287L;

    /**
     * @directed true
     * @link association com.wwm.db.internal.whirlwind.Node
     * @supplierCardinality 0..*
     */
    protected Branch[] children;
    protected int numChildren = 0;

    /**
     * Create a BranchNode to replace the supplied LeafNode and configured to expect numChildren children.
     * We always create BranchNodes to replace a leaf node, apart from the one instance of Index.<br>
     * NOTE: In order to ensure that nothing goes wrong, the leaf attribute reference is set to null
     * as the leaf is expected to be disguarded.  A leaf and branch referencing the same SearchAttributeContainer
     * would not be good!
     *
     * @param parent
     * @param attributes
     * @param numChildren
     */
    public BranchNode( Ref<BranchNode> parentRef, int numChildren) {
        super(parentRef);
        assert(mutable);
        children = new Branch[numChildren];
    }

    /**
     * TODO Does this make sense.. it's only called from Index
     */
    public BranchNode() {
        super();
        assert(mutable);
        children = new Branch[1];
    }

    public BranchNode(BranchNode clonee) {
        super(clonee);
        assert(mutable);
        numChildren = clonee.numChildren;
        if (clonee.children == null) {
            this.children = null;
        } else {
            children = new Branch[clonee.children.length];
            for (int i = 0; i < children.length; i++) {
                if (clonee.children[i] != null) {
                    children[i] = new Branch(clonee.children[i]);
                } else {
                    children[i] = null;
                }
            }
        }
    }


    /**
     * @param child
     * @return The branch annotations. Do not modify.
     */
    // version safe
    public IConstraintMap getBranchAnnotations(RefImpl<? extends Node<?>> child) {
        for (int i = 0; i < numChildren; i++) {
            if (children[i].getChildRef().equals(child)) {
                return children[i].getAnnotations();
            }
        }
        assert(false);
        return null;
    }

    // version safe
    public Branch getNode(int i){
        return children[i];
    }


	@Override
	public void accept(NodeVisitor visitor, NodeStorageManager storage) {
		visitor.visit(this, storage);
	}


    /**
     * @param nodeFind
     *            Node to replace
     * @param nodeReplace
     *            Node to relace it with
     * @return true if found node and replaced it.  false if failed
     */
    // version safe
    public boolean replaceChild(Node nodeFind, Node<?> nodeReplace, NodeStorageManager storage) {
        assert(mutable);

        for (int i = 0; i < numChildren; i++) {
            if (children[i].getChild(storage).getRef().equals(nodeFind.getRef())) {
                BranchNode updateThis = storage.getWriteableBranch(this.getRef());
                updateThis.children[i].setChild(nodeReplace);
                updateThis.setModified();
                return true;
            }
        }
        return false;
    }

    /**
     * Add a new LeafNode configured with supplied attrs.
     * TODO Might be good to check that Attrs are consistent ?? (debug only)
     * @param attrs
     * @return LeafNode
     */
    // version safe
    public LeafNode createLeaf(IAttributeConstraint constraint, int splitId, NodeStorageManager storage) {
        LeafNode newLeaf = storage.createLeafNode(this.getRef());
        BranchNode clonedBranch = storage.getWriteableBranch(this.getRef());
        clonedBranch.add( new Branch((RefImpl)newLeaf.getRef(), constraint, splitId) );
        return newLeaf;
    }


    /**
     * String output is "ref of this node -> [ refs of children ]"
     */
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder().append(super.toString()).append(" -> [ ");
        for (Branch child : children) {
            if (child != null) {
                result.append(System.getProperty( "line.separator")).append(child.toString());
            }
        }
        return result.append(" ]").toString();
    }


    /* (non-Javadoc)
     * @see com.wwm.db.internal.whirlwind.Node#dumpNode(java.io.PrintStream)
     */
    @Override
    // version safe
    public void dumpNode(PrintStream out, int depth, NodeStorageManager storage) {

        out.println( "[" + depth + "] === BranchConstraint ===" );

        for (int i = 0; i < numChildren; i++) {
            Branch branch = children[i];
            branch.dumpNode( out, depth + 1, storage);
        }
    }


    /**
     * Return the number of children this node has
     * @return
     */
    public int getNumChildren() {
        return numChildren;
    }

    /**
     * Add branch to this BranchNode's children array.<br>
     * NOTE: Made private final so that can be inlined by optimiser.
     * @param node Node to add
     */
    // not version safe, caller must clone
    final void add( Branch branch ) {
        setModified();
        assert( numChildren < children.length);
        children[numChildren++] = branch;
    }

    // version safe
    private IAttributeConstraint safeExpandAnnotation(Attribute added, int attrId, int branchIndex, NodeStorageManager storage) {
        BranchNode clone = storage.getWriteableBranch(this.getRef());
        if (branchIndex >= 0) {
            Branch branch = clone.children[branchIndex];
            IConstraintMap atts = branch.getAnnotations();
            IAttributeConstraint na;
            if (added != null) {
                na = clone.expandAnnotations(atts, added, storage);
            } else {
                na = clone.expandNullAnnotations(atts, attrId, storage);
            }
            return na;
        }
        else {
            assert(false);
            return null;
        }
    }

    /**Expand the annotation bubble if possible. Recurse up the tree. Optimise on the way back down.
     * @param added
     * @return This node's constraint, or first ancestor with same type constraint.
     * @throws CloneNotSupportedException
     */
    // version safe
    IAttributeConstraint expandAnnotation(Attribute added, int attrId, Ref fromChild, NodeStorageManager storage) {
        assert(mutable);

        // Find which child branch this reference came from
        int branch = -1;
        for (int i = 0; i < numChildren; i++) {
            if (children[i].getChildRef().equals(fromChild)) {
                branch = i;
                IConstraintMap atts = children[i].getAnnotations();
                IAttributeConstraint na = atts.findAttr(attrId);
                if (na != null) {
                    // If added attr (including null), doesn't expand the constraint, then nothing more to do
                    if (!na.isExpandedBy(added)) {
                        return na;
                    }
                }
            }
        }

        // Now expand the annotation on that branch
        return safeExpandAnnotation( added, attrId, branch, storage);
    }

    /**Recurse up the tree and find the applicable constraint corresponding to the attribute id.
     * @param child
     * @param splitAttrId
     * @return The constraint applicable, or null if there is no constraint anywhere up the tree
     */
    public IAttributeConstraint findConstraint(Node child, int attrId, NodeStorageManager storage) {
        for (int i = 0; i < numChildren; i++) {
            if (children[i].getChild(storage) == child) {	// FIXME: Should compare RefImpls?
                if (children[i].getSplitId() == attrId) {
                    if (children[i].getConstraint() != null) {
                        return children[i].getConstraint();
                    }
                }
            }
        }
        if (getParentRef() == null) {
            return null;
        }
        return getParent(storage).findConstraint(this, attrId, storage);
    }
}