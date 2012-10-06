package com.wwm.db.server.whirlwind.internal;

import java.io.Serializable;
import java.util.Map.Entry;
import java.util.Set;

import org.fuzzydb.attrs.IScoreConfiguration;
import org.fuzzydb.client.marker.IWhirlwindItem;
import org.fuzzydb.core.whirlwind.SearchSpec;
import org.fuzzydb.server.internal.index.Index;
import org.fuzzydb.server.internal.index.WhirlwindIndexManager;



/**
 * Responsible for analysing searches vs IndexStrategy, and choosing
 * which index to use.
 * @author Neale
 *
 */
public class WWSearchOptimiser<T extends IWhirlwindItem> implements Serializable {

    private static final long serialVersionUID = 1L;

    private WhirlwindIndexManager<T> indexManager;


    public WWSearchOptimiser(WhirlwindIndexManager<T> indexManager) {
        this.indexManager = indexManager;
    }


    public WhirlwindIndex<T> getBestIndex(SearchSpec searchSpec, IScoreConfiguration mergedScorers) {
    	// For now, just find the first whirlwindIndex
    	Set<Entry<String,Index<T>>> indexes = indexManager.getIndexes().entrySet();

    	for (Entry<String, Index<T>> entry : indexes) {
			if (entry.getKey().startsWith("@@")){
				return (WhirlwindIndex<T>) entry.getValue();
			}
		}
    	return null;
    }



}
