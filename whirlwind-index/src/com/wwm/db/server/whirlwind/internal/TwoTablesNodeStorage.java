package com.wwm.db.server.whirlwind.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.fuzzydb.client.Ref;
import org.fuzzydb.client.exceptions.UnknownObjectException;
import org.fuzzydb.client.internal.RefImpl;
import org.fuzzydb.core.LogFactory;
import org.fuzzydb.server.internal.table.Table;
import org.fuzzydb.server.internal.table.UserTable;
import org.fuzzydb.server.internal.whirlwind.RefAware;
import org.slf4j.Logger;


/**
 * FIXME: Consider using the isModified() tests to reduce load if necessary.
 *
 * @author Neale
 *
 */
public class TwoTablesNodeStorage implements NodeStorageManager {

	private static final long serialVersionUID = 1L;


	private static final Logger log = LogFactory.getLogger(WhirlwindIndex.class);


	/**
	 * Enable/disable sanity checking, such as checking that an object
	 * we update, actually got modified compared to the visible version
	 * it is supposed to be replacing.
	 */
	private static final boolean debug = false;


	private final WhirlwindIndex index;

	/**
	 * Starting point
	 */
	private final Ref<BranchNode> root;


	/** Table used for underlying implementation */
	private final Table<BranchNode,BranchNode> branchNodes;
	private final Table<LeafNode,LeafNode> leafNodes;

	private final Map<Ref<LeafNode>,LeafNode> pendingCreateLeaf = new TreeMap<Ref<LeafNode>, LeafNode>();
	private final Map<Ref<BranchNode>,BranchNode> pendingCreateBranch = new TreeMap<Ref<BranchNode>, BranchNode>();

	private final Map<Ref<LeafNode>,LeafNode> pendingUpdateLeaf = new TreeMap<Ref<LeafNode>, LeafNode>();
	private final Map<Ref<BranchNode>,BranchNode> pendingUpdateBranch = new TreeMap<Ref<BranchNode>, BranchNode>();

	private final ArrayList<Ref<LeafNode>> pendingDeleteLeaf = new ArrayList<Ref<LeafNode>>();
	private final ArrayList<Ref<BranchNode>> pendingDeleteBranch = new ArrayList<Ref<BranchNode>>();

	public TwoTablesNodeStorage(WhirlwindIndex parent, UserTable<?> table, String instanceName) {
		index = parent;
		leafNodes = WWTableFactory.createLeafWWTable(table.getNamespace(), table.getStoredClass(), instanceName);
		branchNodes = WWTableFactory.createBranchWWTable(table.getNamespace(), table.getStoredClass(), instanceName);

		// TODO: Maybe could add an extra table ..
		// one for stuff used during read: e.g NodeAnnotations, and one for stuff used
		// during write BranchConstraint

		// Create initial node
		BranchNode index = new BranchNode();
		root = branchNodes.allocOneRef();
		branchNodes.create(root, index);

	}


	/* (non-Javadoc)
	 * @see com.wwm.db.internal.whirlwind.NodeStorageManager#deletePeristentData()
	 */
	@Override
	public boolean deletePersistentData() {
    	boolean success = true;
    	success &= leafNodes.deletePersistentData();
    	success &= branchNodes.deletePersistentData();
    	return success;
	}

	/**
	 * Write back all objects that were requested as writable versions (i.e. from insert operations)
	 * and ignore any that are not marked as modified.
	 *
	 * ALGO:
	 *  - record update maps of RefImpl->Object for each underlying table (Leaf, Branch) when we getWritable()
	 *  When writeChanges()
	 *  - record create maps of RefImpl->Object for each table, when we get a new object.
	 *  - iterate over maps
	 *  - use Node.isModified() to find those that need updating
	 *  - update or create them.
	 *
	 *  FIXME: Document answer for following from Skype log to AC:
	 *  (*) Question 1: Can we do the write-behind caching across multiple inserts of the same transaction?
	 *  (*) Question 2: If our write behind cache is ordered by oid (which it should be) then that should be
	 *    				very efficient with the pager... we could perhaps do thousands (or millions) of inserts per transaction.
	 *    Yeah but, no but: The problem we get is that our tables will hold on to all the objects waiting to be written.
	 *    We'd have to cause writeChanges() to be called if we exceed a given size of pending actions...
	 *    For now, we can leave it writing changes to tables after each insert... but could add a batch insert...?
	 */
	@Override
	public void writeChanges() {
		try {
			applyChanges( leafNodes, pendingCreateLeaf, pendingUpdateLeaf, pendingDeleteLeaf);
			applyChanges( branchNodes, pendingCreateBranch, pendingUpdateBranch, pendingDeleteBranch);
		} catch (UnknownObjectException e) {
			throw new Error("Unexpected failure", e);
		}
	}


	private <T extends Node> void applyChanges(Table<T,T> nodes,
								Map<Ref<T>, T> pendingCreate,
								Map<Ref<T>, T> pendingUpdate,
								List<Ref<T>> pendingDelete) throws UnknownObjectException {

//		log.info("-- Updating table: " + nodes.getTableId());

		for ( T createdNode: pendingCreate.values()){
//			if (createdNode.isModified()){
				nodes.create(createdNode.getRef(), createdNode);
//				log.info("Created: " + createdNode.getRef().toString() );
//			}
		}

		for ( T updatedNode: pendingUpdate.values()){
//			if (updatedNode.isModified()){  // FIXME: Find out when this got commented out and why?
				if (debug) {
					checkModified( nodes, updatedNode);
				}
				nodes.update(updatedNode.getRef(), updatedNode);
//				log.info("Updated: " + updatedNode.getRef().toString() );
//			}
		}

		for ( Ref<T> delRef: pendingDelete){
			nodes.delete(delRef);
//			log.info("Deleted: " + delRef.toString() );
		}

		pendingCreate.clear();
		pendingUpdate.clear();
		pendingDelete.clear();
	}

	@SuppressWarnings("unused") // test function that is sometimes used
	private <T> void checkModified(Table<T,T> nodes, RefAware<T> node) {
		try {
			T prev = nodes.getObject(node.getRef());
			if ( prev.equals(node)){
				log.warn("Updated node contained no modifications");
			}
		} catch (UnknownObjectException e) {
			throw new Error(e); // should find it.
		}

	}


	/* (non-Javadoc)
	 * @see com.wwm.db.internal.whirlwind.NodeStorageManager#getRootNode()
	 */
    @Override
	public BranchNode getRootNode() {
    	BranchNode rootNode = null;
		try {
			rootNode = branchNodes.getObject( root );
		} catch (UnknownObjectException e) {
			throw new Error(e);
		}
		return rootNode;
    }

    /* (non-Javadoc)
     * @see com.wwm.db.internal.whirlwind.NodeStorageManager#getWritableRoot()
     */
    @Override
	public BranchNode getWritableRoot() {
    	return getWriteableBranch( root );
    }


	@Override
	public Node<?> getNode(Ref<? extends Node> ref) {
		Node<?> node;
		try {
			if (((RefImpl<?>) ref).getTable() == branchNodes.getTableId()){
				node = branchNodes.getObject((Ref<BranchNode>) ref);
			} else {
				node = leafNodes.getObject((Ref<LeafNode>) ref);
			}
		} catch (UnknownObjectException e) {
			throw new Error("Wasn't expecting missing object");
		}
		return node;
	}

	@Override
	public BranchNode getBranchNode(Ref<BranchNode> ref) {
		BranchNode node;
		try {
			node = branchNodes.getObject(ref);
		} catch (UnknownObjectException e) {
			throw new Error("Wasn't expecting missing object:" + ref.toString());
		}
		return node;
	}




	@Override
	public boolean isWritable(BranchNode node) {
			try {
				return branchNodes.canSeeLatest(node.getRef());
			} catch (UnknownObjectException e) {
				throw new Error(e);
			}
	//		ObjectVersion ov = table.retrieve(node.getRef(), repos.getCurrentVersion());
	//		return (ov == null || ov.getId().getObjVersion() < node.getDbid().getObjVersion());
		}

	@Override
	public boolean isWritable(LeafNode node) {
			try {
				return leafNodes.canSeeLatest(node.getRef());
			} catch (UnknownObjectException e) {
				throw new Error(e);
			}
	//		ObjectVersion ov = table.retrieve(node.getRef(), repos.getCurrentVersion());
	//		return (ov == null || ov.getId().getObjVersion() < node.getDbid().getObjVersion());
		}

	@Override
	public BranchNode createBranchNode(Ref<BranchNode> parentRef, int numChildren) {
		BranchNode node = new BranchNode(parentRef, numChildren);
		Ref<BranchNode> newRef = branchNodes.allocOneRefNear(parentRef);
		node.setRef(newRef);
		pendingCreateBranch.put(newRef, node);
		return node;
	}


	@Override
	public LeafNode createLeafNode(Ref<BranchNode> parent) {
		LeafNode leaf = new LeafNode(parent);
		Ref<LeafNode> setRef = leafNodes.allocOneRef();
		leaf.setRef(setRef);
		pendingCreateLeaf.put(setRef, leaf);
		return leaf;
	}


	@Override
	public void deleteNode(Ref<? extends Node> ref) {
		if (((RefImpl) ref).getTable() == branchNodes.getTableId()){
			pendingDeleteBranch.add((Ref<BranchNode>) ref);
		} else {
			pendingDeleteLeaf.add((Ref<LeafNode>) ref);
		}
	}


	@Override
	public WhirlwindIndex getIndex() {
		return index;
	}


	@Override
	public BranchNode getWriteableBranch(Ref<BranchNode> ref) {
		// Most likely is that we've got it pending an update
		BranchNode node = pendingUpdateBranch.get(ref);
		if ( node != null){
			return node;
		}
		// Or we might be doing a get, when we've actually only just created it.
		node = pendingCreateBranch.get(ref);
		if ( node != null){
			return node;
		}
		// Else.. we've not retrieved it for write, yet, so get it and add to pending
		node = new BranchNode( getBranchNode(ref) );
		pendingUpdateBranch.put(ref, node);
		return node;
	}


	@Override
	public LeafNode getWriteableLeaf(Ref<LeafNode> ref) {
		// Most likely is that we've got it pending an update
		LeafNode node = pendingUpdateLeaf.get(ref);
		if ( node != null){
			return node;
		}
		// Or we might be doing a get, when we've actually only just created it.
		node = pendingCreateLeaf.get(ref);
		if ( node != null){
			return node;
		}
		// Else.. we've not retrieved it for write, yet, so get it and add to pending
		node = new LeafNode( (LeafNode)getNode(ref) );
		pendingUpdateLeaf.put(ref, node);
		return node;
	}

	@Override
	public Node getWriteableNode(Ref<? extends Node> ref) {
		if (((RefImpl) ref).getTable() == branchNodes.getTableId()){
			return getWriteableBranch((Ref<BranchNode>) ref);
		}
		return getWriteableLeaf((Ref<LeafNode>) ref);
	}
}
