package com.wwm.db.server.whirlwind.internal;

import java.util.Random;

import org.fuzzydb.client.internal.RefImpl;
import org.fuzzydb.client.marker.IWhirlwindItem;
import org.fuzzydb.core.marker.IAttributeContainer;

/**
 * InsertVisitor attempts to add the IWhirlwindItem to the tree below a given node.
 *
 * FIXME (review) If it will not fit in this part of the tree, the visit fails.***
 * The normal method of insertion is to iterate over all child nodes attempting to
 * insert, until an insert() call returns true.
 */
public class InsertVisitor implements NodeVisitor {

    private static Random rand = new Random();

    private final IWhirlwindItem item;

    /**
     * @param item
     * 			The item to attempt to add to this part of the subtree.
     */
	public InsertVisitor(IWhirlwindItem item) {
		this.item = item;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void visit(BranchNode branchNode, NodeStorageManager storage) {
		assert branchNode.mutable;

		Node<? extends Node<?>> bestChild = findWritableBestBranch(branchNode, item, storage);

        if (bestChild != null) {
            bestChild.accept(this, storage);
            return;
        }

        // Exceptional case - empty node. So do this last
        if (branchNode.getNumChildren() == 0) {
            // We are making a change so clone the node
            BranchNode updateBranch = storage.getWriteableBranch(branchNode.getRef());
            LeafNode newLeaf = storage.createLeafNode(updateBranch.getRef());
            updateBranch.add( new Branch( (RefImpl)newLeaf.getRef()) );
            newLeaf.accept(this, storage);
            return;
        }
        // FIXME: This happens if an enum splitter has been configured too small
        // we shouldn't need to pre-allocate the branches in that case
        throw new Error("InsertFailedException(item);");
	}

    private Node<? extends Node<?>> findWritableBestBranch(BranchNode branchNode, IAttributeContainer item, NodeStorageManager storage) {

    	Node<? extends Node<?>> bestChild = null;
        int bestCount = Integer.MAX_VALUE;
        for (int i = 0; i < branchNode.numChildren; i++) {
            if (branchNode.children[i].consistent(item)) {
                Node<? extends Node<?>> child = branchNode.children[i].getWriteableChild(storage);

                // normal case is that we split, so first matching child is appropriate
                if (branchNode.children[i].getSplitId() != -1){ // i.e. hasSplit()
                    return child;
                }

                if (child instanceof LeafNode) {
                	LeafNode leaf = (LeafNode) child;
                    // If children are LeafNode, and more than one qualifies, insert into the smallest one.
                    // This is because we can split a node without splitting on an attribute, if the attributes are
                    // too close to split and no manager wants to split.
                    int count = leaf.items.size();
                    if (count < bestCount) {
                        bestCount = count;
                        bestChild = child;
                    }
                } else {
                    int bestIndex = rand.nextInt(branchNode.numChildren); // rand between 0 to numChildren-1
                    return branchNode.children[bestIndex].getWriteableChild(storage);

                    // was     			if (bestChild == null) bestChild = child;	// prefer leaf node to branch node if both qualify
                    //        			break;
                }
            }
        }
        return (bestChild == null) ? null : storage.getWriteableNode( bestChild.getRef() );
    }


	@Override
	public void visit(LeafNode leafNode, NodeStorageManager storage) {
		assert(leafNode.mutable);
		assert(!leafNode.contains(item));	// Must not insert same thing more than once!
		leafNode.safeInsert(item, storage);
	}

}
