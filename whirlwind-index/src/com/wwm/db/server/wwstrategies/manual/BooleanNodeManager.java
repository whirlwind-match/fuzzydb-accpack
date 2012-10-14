/*
 * Created on 14-Nov-2004
 *
 */
package com.wwm.db.server.wwstrategies.manual;

import org.fuzzydb.attrs.bool.BooleanConstraint;

import com.wwm.db.server.whirlwind.internal.BranchNode;
import com.wwm.db.server.whirlwind.internal.LeafNode;
import com.wwm.db.server.whirlwind.internal.NodeStorageManager;


/**
 * @author Neale Upstone
 */
public class BooleanNodeManager extends NodeManager {
//    private static Logger log = Logger.getLogger(BooleanNodeManager.class
//            .getName());
    
	/**
	 * @param date
	 */
	public BooleanNodeManager(int splitAttrId) {
	    super( splitAttrId );
	}

    
    /* (non-Javadoc)
     * @see com.wwm.db.internal.whirlwind.NodeManager#createReplacementBranchNode(com.wwm.db.internal.whirlwind.LeafNode, com.wwm.db.internal.whirlwind.NodeStorageManager)
     */
    @Override
	public BranchNode createReplacementBranchNode(LeafNode leaf, NodeStorageManager storage ) {

        // For now, we're expecting to split into 2
        // TODO ? Change so that we split into 4 equal areas between the min and max that were found
        branch = storage.createBranchNode( leaf.getParentRef(), 3 );
        
        createNodes(storage);

        return branch;
    }
	
    
    /**
     * Populate the nodes of this branch
     * @param currentAttrs 
     * @param children
     */
	private void createNodes(NodeStorageManager storage) {
        BooleanConstraint isTrue, isFalse;
	    isTrue = new BooleanConstraint( splitAttrId, true);
	    isFalse = new BooleanConstraint( splitAttrId, false );

	    createNewLeaves(isTrue, isFalse, storage);
	}
}
