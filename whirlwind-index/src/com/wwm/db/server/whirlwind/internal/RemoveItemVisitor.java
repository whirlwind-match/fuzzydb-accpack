package com.wwm.db.server.whirlwind.internal;

import org.fuzzydb.core.marker.IAttributeContainer;

public class RemoveItemVisitor implements NodeVisitor {

	private IAttributeContainer item;
	private boolean succeeded = false;

	public RemoveItemVisitor(IAttributeContainer item) {
		this.item = item;
	}

	public void visit(BranchNode branchNode, NodeStorageManager storage) {

        // NOTE: This operation does not modify the branch node.  Just the eventual leaf
        for (int i = 0; i < branchNode.numChildren; i++) {
            if (branchNode.children[i].consistent(item)) {
                Node<?> clone = branchNode.children[i].getWriteableChild(storage);
                clone.accept(this, storage); // attempt to remove here
                if (succeeded) {
                    return;
                }
            }
        }
        // not so far on this one - possible to fall through if was once a big leaf/no splitters
	}

	public void visit(LeafNode leafNode, NodeStorageManager storage) {
		// assert(mutable);
        for (int i = 0; i < leafNode.items.size(); i++) {
        	if (item == leafNode.items.get(i)) {
        		LeafNode clone = storage.getWriteableLeaf(leafNode.getRef());
        		IAttributeContainer removed = clone.items.remove(i);
        		clone.setModified(); // indicate that clone needs writing
        		assert(removed == item);
        		succeeded = true;
        		return;
        	}
        }
        // completing loop means we failed on this one - acceptable if we've split a big node
	}

	public boolean getSucceeded() {
		return succeeded;
	}

}
