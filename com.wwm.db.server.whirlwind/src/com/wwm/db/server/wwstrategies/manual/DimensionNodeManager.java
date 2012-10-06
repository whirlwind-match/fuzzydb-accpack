/*
 * Created on 23-Nov-2004
 *
 * (C) Copyright 2004, TheOne, Adrian Clarkson & Neale Upstone.  All rights reserved.
 */
package com.wwm.db.server.wwstrategies.manual;

import org.fuzzydb.attrs.dimensions.DimensionsNodeSelector;

import com.wwm.db.server.whirlwind.internal.BranchNode;
import com.wwm.db.server.whirlwind.internal.LeafNode;
import com.wwm.db.server.whirlwind.internal.NodeStorageManager;


/**
 * @author Neale
 */
public class DimensionNodeManager extends NodeManager {

    int splitIndex;

    float splitVal;

    /**
     * @param selectorClass
     * @param splitVal
     * @param splitIndex
     *            e.g. 0 for X, 1 for Y, 2 for Z
     */
    public DimensionNodeManager(int splitAttrId, float splitVal,
            int splitIndex, int numDimensions) {
        super(splitAttrId);
        this.splitIndex = splitIndex;
        this.splitVal = splitVal;
    }

    /*
     * (non-Javadoc)
     * 
     * @see likemynds.db.indextree.NodeManager#createReplacementBranchNode(likemynds.db.indextree.LeafNode)
     */
    @Override
    public BranchNode createReplacementBranchNode(LeafNode leaf, NodeStorageManager storage) {
        branch = storage.createBranchNode(leaf.getParentRef(), 3);

        DimensionsNodeSelector low = new DimensionsNodeSelector( splitAttrId, splitIndex, false, splitVal);
        DimensionsNodeSelector high = new DimensionsNodeSelector( splitAttrId, splitIndex, true, splitVal);

        createNewLeaves(low, high, storage);
        return branch;
    }

}
