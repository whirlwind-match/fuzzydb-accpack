package com.wwm.db.server.whirlwind.internal;


import java.io.Serializable;

import org.fuzzydb.client.internal.MetaObject;
import org.fuzzydb.client.internal.RefImpl;
import org.fuzzydb.client.marker.IWhirlwindItem;
import org.fuzzydb.core.whirlwind.internal.AttributeCache;
import org.fuzzydb.server.internal.index.Index;
import org.fuzzydb.server.internal.table.UserTable;
import org.slf4j.Logger;

import com.wwm.db.server.wwstrategies.manual.StrategyManager;


/**
 * WhirlwindIndex exposes the ability to insert WhirlwindItems into an index formed 
 * as a structure of linked Nodes.
 * Beyond the initial insert, it is the IndexStrategy of this index that determines
 * how the index is organised.
 * Searching the index is an externally driven feature, which should only require
 * a starting point, which is provided by getRootNode().  The external Search
 * will then navigate the index, which it will hopefully find organised optimally
 * due to the IndexStategy having been supplied externally.   
 */
public class WhirlwindIndex<T extends IWhirlwindItem> implements Index<T>, Serializable {
	private static final long serialVersionUID = 1L;

	/**
	 * Manages storage of nodes on behalf of this index.
	 */
	private NodeStorageManager storage;
	
	// TODO private NodeStorageManager newStorage; // for an index under construction
	

	private UserTable<T> parent;
	
	private StrategyManager strategyManager; // Manages how we go about organising the index to be efficient for the expected searches.

	private boolean built = false; // set true when up to date.
	
	
	/**
	 * 
	 * @param parent - the parent ClassTable
	 */
	public WhirlwindIndex(UserTable<T> parent, StrategyManager manager) {
		super();
		this.parent = parent;
		this.strategyManager = manager;

		storage = new TwoTablesNodeStorage(this, parent, strategyManager.getStrategy().getName());

	}

	
    public void initialise() {
    	
    	// TODO: Kick off a background process if table is huge.
		if (!built){
			build(); 
			built = true;
		}
	}
    
    public boolean deletePersistentData() {
    	boolean success = true;
    	success &= storage.deletePersistentData();
//    	success &= newStorage.deletePeristentData();
    	return success;
    }
	
	private void build() {
		
		getLog().info( "= Building @" + parent.getStoredClass().getName() + "@" + strategyManager.getStrategy().getName() + "... =");
		int countInserted = 0;
		for (MetaObject<T> entry : parent) {
			insert(entry.getRef(), entry.getObject() );
			countInserted++;
		}
		getLog().info(" Completed @" + parent.getStoredClass().getName() + "@" + strategyManager.getStrategy().getName() + " - " 
				+ countInserted + " items inserted" );
		getLog().info(" AttributeCache has " + getAttributeCache().getSize() 
				+ " entries, which yielded " + getAttributeCache().getSuccesses() + " merges.");
	}

	
	public void testInsert(RefImpl<T> ref, T item) {
		// can always insert, so we return without throwing an exception
	}

	
	public void insert(RefImpl<T> ref, T item) {
		// TODO: unify attribute instances with those already in being stored
		// we still want to do this as it allows us to get more pages in memory.
//		((DbTreeItem)o.getItem()).switchTo( getAttributeCache() );	

		BranchNode clone = storage.getWritableRoot();
		clone.accept(new InsertVisitor(item), storage);
		
		// Now flush the changes
		storage.writeChanges();
	}

	public void remove(RefImpl<T> ref, T item) {
		BranchNode clone = storage.getWritableRoot();
		RemoveItemVisitor visitor = new RemoveItemVisitor(item);
		clone.accept( visitor, storage);
		boolean success = visitor.getSucceeded();
        assert(success);	// remove an item failed, not found

        // Now flush the changes
		storage.writeChanges();
	}
	
//	public Search newSearch(SearchSpec spec, boolean nominee)
//	{
//		updateConfiguration();
//		return new Search(spec, nominee, this);
//	}
	
	/**
	 * Not valid for DB2
	 * This used to only happen after index build, or repository restore.. or something.. but DBv1 doesn't do it anyway.
	 * Our strategy of attribute caching has to work with paged tables and such... and what we'll do is to store an
	 * attribute cache on our Namespace, and use that for giving us smaller and faster pages. 
	 */
	public void rebuildAttributeCache(AttributeCache attributeCache) {
		assert(false);
	}
	

	private Logger getLog() {
		return parent.getNamespace().getLog();
	}

	private AttributeCache getAttributeCache() {
		return parent.getNamespace().getAttributeCache();
	}


	public StrategyManager getStrategyManager() {
		return strategyManager;
	}

	public NodeStorageManager getStorage() {
		return storage;
	}
}
