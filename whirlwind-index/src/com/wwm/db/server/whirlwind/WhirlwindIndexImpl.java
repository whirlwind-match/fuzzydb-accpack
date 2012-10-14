package com.wwm.db.server.whirlwind;

import java.util.Collection;

import org.fuzzydb.attrs.IScoreConfiguration;
import org.fuzzydb.attrs.WhirlwindConfiguration;
import org.fuzzydb.attrs.search.SearchSpecImpl;
import org.fuzzydb.client.marker.IWhirlwindItem;
import org.fuzzydb.client.whirlwind.IndexStrategy;
import org.fuzzydb.core.whirlwind.SearchSpec;
import org.fuzzydb.server.internal.index.IndexImplementation;
import org.fuzzydb.server.internal.index.WhirlwindIndexManager;
import org.fuzzydb.server.internal.search.Search;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wwm.db.server.whirlwind.internal.OrderedSearch;
import com.wwm.db.server.whirlwind.internal.WWSearchOptimiser;
import com.wwm.db.server.whirlwind.internal.WhirlwindIndex;
import com.wwm.db.server.wwstrategies.manual.StrategyManager;


public class WhirlwindIndexImpl implements IndexImplementation {

	private final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public <T extends IWhirlwindItem> void detectIndex(WhirlwindIndexManager<T> indexManager, WhirlwindConfiguration conf) {
		// TODO:
//		Need to add args for table, and also support two different impls of a WWIndex (Dumb and clever)..a.
//		which are in WhirlwindIndexManager.

        Collection<IndexStrategy> strategies = conf.getIndexStrategies();

        // Loop over each strategy and make sure we have an index for each one.
        for (IndexStrategy strategy : strategies) {
            checkWWIndex(strategy, indexManager);
        }
	}

    /**
     * Check if an index exists for the given strategy, whether it needs rebuilding (TODO),
     * etc.
     * @param strategy
     */
    private <T extends IWhirlwindItem> void checkWWIndex(IndexStrategy strategy, WhirlwindIndexManager<T> indexManager) {
        String name = strategy.getName();
        WhirlwindIndex<T> index = (WhirlwindIndex<T>) indexManager.getWWIndex(name);
        if (index == null){
            index = new StrategyManager(strategy).createIndex(indexManager.getTable());
            indexManager.addWWIndex(name, index);
        }
    }

	@Override
	public <T extends IWhirlwindItem> Search getSearch(SearchSpec searchSpec, IScoreConfiguration mergedScorers,
			IScoreConfiguration config, boolean wantNominee, WhirlwindIndexManager<T> indexManager) {

        WWSearchOptimiser<T> searchOptimiser = new WWSearchOptimiser<T>( indexManager );

		WhirlwindIndex<T> index = searchOptimiser.getBestIndex( searchSpec, mergedScorers );
		if (index == null) {
			log.warn("FALLING BACK TO FULL TABLE SCAN for matching.\n" +
					"Whirlwind found no index for {},  perhaps you need to configure some index strategies...",
					searchSpec.getClazz().getName() );
			return null;
		}
        Search search = new OrderedSearch((SearchSpecImpl) searchSpec, config, wantNominee, index.getStorage() );
		return search;
	}
}
