/*
 * Created on 14-Nov-2004
 *
 */
package com.wwm.db.server.wwstrategies.manual;

import org.fuzzydb.attrs.simple.FloatConstraint;
import org.fuzzydb.attrs.simple.FloatValue;
import org.fuzzydb.core.whirlwind.internal.IAttributeConstraint;

import com.wwm.db.server.whirlwind.internal.BranchNode;
import com.wwm.db.server.whirlwind.internal.LeafNode;
import com.wwm.db.server.whirlwind.internal.NodeStorageManager;


/**
 * @author Neale Upstone
 */
public class FloatNodeManager extends NodeManager {

    // private static Logger log = Logger.getLogger(FloatNodeManager.class.getName());
    

	protected float splitValue;

	/**
	 * @param isHave
	 * @param date 
	 */
	public FloatNodeManager(int splitAttrId, float value) {
	    super( splitAttrId );
	    splitValue = value;
	}

    
    /* (non-Javadoc)
     * @see likemynds.db.indextree.NodeManager#createBranchNode(likemynds.db.indextree.BranchNode)
     */
    @Override
	public BranchNode createReplacementBranchNode(LeafNode leaf, NodeStorageManager storage) {

        // For now, we're expecting to split into 2
        // TODO ? Change so that we split into 4 equal areas between the min and max that were found
        branch = storage.createBranchNode( leaf.getParentRef(), 3 );
        
        createNodes(leaf.getParent(storage).findConstraint(leaf, splitAttrId, storage), storage);

        return branch;
    }

    
    /**
     * Populate the nodes of this branch
     * @param children
     */
	private void createNodes(IAttributeConstraint currentConstraint, NodeStorageManager storage) {

	    FloatConstraint low, high;
	    	    
	    FloatConstraint parentRange = (FloatConstraint) currentConstraint;
	    
	    // if there was a DobRangeHave specified in parent, then split within that, otherwise use limits
	    
	    // TODO: Change so creates instance and then gets sub-range, as we do for Point3DRangeConstraint
	    if ( parentRange != null ){
		    low = new FloatConstraint( splitAttrId, parentRange.getMin(), splitValue );
		    high = new FloatConstraint( splitAttrId, splitValue, parentRange.getMax());
	    }
	    else {
		    low = new FloatConstraint( splitAttrId, -Float.MAX_VALUE, splitValue );
		    high = new FloatConstraint( splitAttrId, splitValue, Float.MAX_VALUE );
	    }
	    
	    createNewLeaves(low, high, storage);
	}

	
	@Override
	public String toString() {
	    return super.toString() + " split at: " + new FloatValue( splitAttrId, splitValue ).toString();
	}

}
