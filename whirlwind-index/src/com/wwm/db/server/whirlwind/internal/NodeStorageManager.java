package com.wwm.db.server.whirlwind.internal;

import java.io.Serializable;

import org.fuzzydb.client.Ref;

/**
 * Manages the storage of nodes for a WhirlwindIndex.
 *
 * An example of different storage strategies is that, we might have a single underlying table for
 * LeafNodes and BranchNodes, or seperate tables.
 *
 * The NodeStorageManager could also be extended to provide a write-behind caching function, as
 * it has control of all persistent storage for the index.
 *
 * It could also be used to map the storage of nodes onto pretty much any underlying medium,
 * including in-memory storage.
 *
 * @author Neale
 *
 */
public interface NodeStorageManager extends Serializable {

	/**
	 * Get the parent index that this is operating on behalf of
	 * @return
	 */
	public WhirlwindIndex<?> getIndex();


	/**
	 * If the implementation implements caching, then writes any changes of
	 * modified objects (i.e. those that are mutable) to the relevant
	 * store.
	 */
	public void writeChanges();


	/**
	 * Expose the root of this index, such that anything that navigates it can
	 * find the starting point.
	 * @return the root Node.  This should be treated as read-only, and
	 * not modified.
	 */
	public BranchNode getRootNode();

	public BranchNode getWritableRoot();

	/**
	 * Get the node for the given RefImpl.
	 *
	 */
	public Node<?> getNode(Ref<? extends Node> ref);

	/**
	 *  Used when we know we're getting a branch node (e.g. if it's the parent of a node)
	 */
	public BranchNode getBranchNode(Ref<BranchNode> ref);


	/**Determines if the node can be modified without updating it. Nodes can be modified if their version is newer than the latest issued version,
	 * this condition happens if the node has already been updated once by the current write transaction.
	 * Nodes which are visible to read transactions cannot be modified.
	 * @param node
	 * @return
	 */
	public boolean isWritable(BranchNode node);

	/**Determines if the node can be modified without updating it. Nodes can be modified if their
	 * version is newer than the latest issued version, this condition happens if the node has
	 * already been updated once by the current write transaction.
	 * Nodes which are visible to read transactions cannot be modified.
	 * @param node
	 * @return
	 */
	public boolean isWritable(LeafNode node);

	/**
	 * Create a new BranchNode, preferably allocated to a reference that
	 * is near parentRef (such that they might be on the same page for
	 * paged tables)
	 */
	public BranchNode createBranchNode(Ref<BranchNode> parentRef, int numChildren);


	/**
	 * Create a brand new LeafNode, with it's parent set to parent.
	 * @return LeafNode with Ref set to newly allocated Ref
	 */
	public LeafNode createLeafNode(Ref<BranchNode> parent);


	public void deleteNode(Ref<? extends Node> ref);


	/**
	 * Gets a writable clone of the referenced BranchNode
	 * @param ref
	 * @return
	 */
	public BranchNode getWriteableBranch(Ref<BranchNode> ref);


	/**
	 * Gets a writable clone of the referenced LeafNode
	 * @param ref
	 * @return
	 */
	public LeafNode getWriteableLeaf(Ref<LeafNode> ref);


	public Node<? extends Node<?>> getWriteableNode(Ref<? extends Node> ref);


	/**
	 * Permanently delete all persistent data being managed by this IndexManager
	 * @return true if succeeded
	 */
	public boolean deletePersistentData();



}