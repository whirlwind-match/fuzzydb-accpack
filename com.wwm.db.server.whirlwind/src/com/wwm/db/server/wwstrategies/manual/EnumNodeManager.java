package com.wwm.db.server.wwstrategies.manual;


import org.fuzzydb.attrs.internal.BranchConstraint;
import org.fuzzydb.core.Settings;

import com.wwm.db.server.whirlwind.internal.BranchNode;
import com.wwm.db.server.whirlwind.internal.LeafNode;
import com.wwm.db.server.whirlwind.internal.NodeStorageManager;

/**
 * @author ac
 *
 */
public class EnumNodeManager extends NodeManager {

	int numEnumValues;

	/**
	 * @param splitAttrId
	 * @param numEnumValues the number of ways this node should split
	 */
	public EnumNodeManager(int splitAttrId, int numEnumValues) {
	    super( splitAttrId);
	    this.numEnumValues = numEnumValues;
	}


	/* (non-Javadoc)
	 * @see likemynds.db.indextree.NodeManager#createReplacementBranchNode(likemynds.db.indextree.LeafNode)
	 */
	@Override
	public BranchNode createReplacementBranchNode(LeafNode leaf, NodeStorageManager storage) {
		int branchCount = numEnumValues + 1;
		BranchNode branch = storage.createBranchNode( leaf.getParentRef(), branchCount );

        for (short enumIndex=0; enumIndex < numEnumValues; enumIndex++) {
        	createEnumBranch(branch, enumIndex, storage); //FIXME: Surely it would be better to do this as a lazy operation (saves scoring empty nodes, and saves memory, and saves on time doing split)
        }

        // null branch for items that are missing the enum
	    branch.createLeaf( null, splitAttrId, storage );

        return branch;
	}


	private void createEnumBranch(BranchNode branch, short enumIndex, NodeStorageManager storage) {

		BranchConstraint con;

        switch ( Settings.getInstance().getScorerVersion() ){
        case compact:	// fallthru
        case v2:
        	con = new org.fuzzydb.attrs.enums.EnumExclusiveValue(splitAttrId, (short)-1, enumIndex).createAnnotation();
        	break;
        default:
        	throw new UnsupportedOperationException();
        }

        branch.createLeaf( con, splitAttrId, storage );
	}

}
