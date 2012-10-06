package com.wwm.db.server.whirlwind.internal;

/**
 * Counts the number of LeafNodes in the tree below the first item.
 */
public class NodeCountVisitor implements NodeVisitor {
	
	private int count = 0;
	
	public void visit(BranchNode branchNode, NodeStorageManager storage) {

        for (int i = 0; i < branchNode.numChildren; i++) {
            Branch branch = branchNode.children[i];
            branch.getChild(storage).accept(this, storage);

        }
	}

	public void visit(LeafNode leafNode, NodeStorageManager storage) {
		count++;
	}
}
