package com.wwm.db.server.whirlwind.internal;


/**
 * Usage:
 *      ShowSplitsVisitor v = new ShowSplitsVisitor();
 *      rootNode.accept(v,storage);
 *      v.toString();
 */
public class ShowSplitsVisitor implements NodeVisitor {

	private StringBuilder str = new StringBuilder();
	
	public void visit(BranchNode branchNode, NodeStorageManager storage) {
        // this split is given by the split Attr Id of an arbitrary child (as those branches were all split on same attr)
        Branch childBranch = branchNode.children[0];
        assert(childBranch != null); // Got a problem if it is
        int attrId = childBranch.getSplitId();
        String thisSplit = "Attr(" + attrId + ")"; // default, but we're going to look for a decorator

        // Say what we split on (unless we're root)
        if ( attrId != -1) {
            //    		Decorator decorator = storage.getConf().getDecorator( attrId );
            //			if ( decorator != null ) {
            //				thisSplit = decorator.getAttrName();
            //			}
        }

        if (branchNode.getParentRef() == null) {
            return;
        }
        
        // Call each parent as we get the split order, we then concatenate from the top down as we return.
        branchNode.getParent(storage).accept(this, storage); // Calls
		str.append(" -> ").append(thisSplit);
	}

	public void visit(LeafNode leafNode, NodeStorageManager storage) {
		if ( leafNode.getParentRef() == null) return;
		
		leafNode.getParent(storage).accept(this, storage);
	}

	
	@Override
	public String toString() {
		return str.toString();
	}
}
