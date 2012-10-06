package com.wwm.db.server.whirlwind.internal;

import org.fuzzydb.client.marker.IWhirlwindItem;
import org.fuzzydb.core.LogFactory;
import org.slf4j.Logger;

public class LeafSplitVisitor implements NodeVisitor {

	private static Logger log = LogFactory.getLogger( LeafSplitVisitor.class );


	@Override
	public void visit(BranchNode branchNode, NodeStorageManager storage) {
		throw new UnsupportedOperationException();
	}

    static private boolean inSplitNode = false; // DEBUG ONLY: For stopping if we split twice.
    /**
     * Split this leaf node into a branch node containing two or more leaf nodes
     * @param leafNode
     * @throws InsertFailedException
     * @throws CloneNotSupportedException
     *
     */
    @Override
	public void visit(LeafNode leafNode, NodeStorageManager storage) {
        if (inSplitNode) throw new Error("Double Entry error in splitNode()");
        inSplitNode = true;

        if( log.isDebugEnabled() ) {
        	log.debug( "Splitting LeafNode: " + this );
        }

        // Find appropriate NodeManager to manage a branch node containing our items
        BranchNode newBranchNode = storage.getIndex().getStrategyManager().createBranchNode( leafNode, storage );

        BranchNode parent = storage.getWriteableBranch( leafNode.getParentRef() );
        //NodeAttributeContainer parentAnnotations = parent.getBranchAnnotations(this.getRef());

        parent.replaceChild( leafNode, newBranchNode, storage );  // should mean we now have no references to us, unless in a search so will eventually get GC'ed

        // Insert items into branch
        for (IWhirlwindItem item : leafNode.items) {
        	newBranchNode.accept( new InsertVisitor(item) , storage);
        }
        storage.deleteNode(leafNode.getRef());
        inSplitNode = false;
    }


}
