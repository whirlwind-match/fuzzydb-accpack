/*
 * Created on 14-Nov-2004
 *
 * (C) Copyright 2004, TheOne, Adrian Clarkson & Neale Upstone.  All rights reserved.
 */
package com.wwm.db.server.wwstrategies.manual;

import org.fuzzydb.core.whirlwind.internal.IAttributeConstraint;

import com.wwm.db.server.whirlwind.internal.BranchNode;
import com.wwm.db.server.whirlwind.internal.LeafNode;
import com.wwm.db.server.whirlwind.internal.NodeStorageManager;




/**
 * @author Neale
 */
public abstract class NodeManager {

    protected BranchNode branch;
    protected int splitAttrId;

    
    /**
     * Select which child node this item goes in.
     * @param item DbTreeItem that the appropriate node is required for
     * @return
     */
//    public abstract Node getNode( DbTreeItem item );
    
    /**
     * @param isHave
     */
    public NodeManager(int splitAttrId) {
    	this.splitAttrId = splitAttrId;
    }

    /**
     * Creates and configures a branchNode according to the split criteria that this 
     * NodeManager knows about
     * @param leaf BranchNode
     * @param dbversion 
     * @param index 
     * @return BranchNode
     */
    public abstract BranchNode createReplacementBranchNode(LeafNode leaf, NodeStorageManager storage );
 
    /**
     * @param low BranchConstraint
     * @param high BranchConstraint
     */
    protected void createNewLeaves(IAttributeConstraint low, IAttributeConstraint high, NodeStorageManager storage) {
	    branch.createLeaf( low, splitAttrId, storage);
	    branch.createLeaf( high, splitAttrId, storage);
	    branch.createLeaf( null, -1, storage);	// leaf for items with missing attribute
    }
    
    
    @Override
	public String toString() {
        return "Attr(" + splitAttrId + ")";
    }
    
}
