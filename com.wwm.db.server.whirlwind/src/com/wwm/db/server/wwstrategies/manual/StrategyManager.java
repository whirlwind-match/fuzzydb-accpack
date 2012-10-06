package com.wwm.db.server.wwstrategies.manual;

import java.io.Serializable;
import java.util.TreeMap;

import org.fuzzydb.attrs.ManualIndexStrategy;
import org.fuzzydb.attrs.SplitConfiguration;
import org.fuzzydb.client.marker.IWhirlwindItem;
import org.fuzzydb.client.whirlwind.IndexStrategy;
import org.fuzzydb.core.LogFactory;
import org.fuzzydb.server.internal.table.UserTable;
import org.slf4j.Logger;

import com.wwm.db.server.whirlwind.internal.BranchNode;
import com.wwm.db.server.whirlwind.internal.LeafNode;
import com.wwm.db.server.whirlwind.internal.NodeStorageManager;
import com.wwm.db.server.whirlwind.internal.WhirlwindIndex;



/**
 * Perhaps strategy manager would be better.
 *
 * @author Neale
 *
 */
public class StrategyManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private static Logger log = LogFactory.getLogger(StrategyManager.class);

    private final IndexStrategy strategy;

    private TreeMap<Integer, AbstractAttributeSplitter> splitters = null;


    public StrategyManager(IndexStrategy strategy) {
        this.strategy = strategy;
    }


    /**
     * Creates a WhirlwindIndex, pre-configured to use this stragegy.
     * @return
     */
    public <E extends IWhirlwindItem> WhirlwindIndex<E> createIndex(UserTable<E> table) {
        return new WhirlwindIndex<E>(table, this);
    }

    /**
     * REVIEW: This seems a bit specific for exposing here... as it ties the indexStrategy to
     * Nodes.  But... if you delve deeper you'll prob find that we're stuck with Nodes as the
     * highest level of abstraction :O)
     * @param node
     * @param storage
     * @return
     */
    public BranchNode createBranchNode(LeafNode leaf, NodeStorageManager storage) {
        NodeManager manager = getBestNodeManager( leaf );

        if (manager != null) {
            BranchNode branch = manager.createReplacementBranchNode( leaf, storage );
            return branch;
        }

        // otherwise no split was possible, create a pair of unconstrained leafs
        BranchNode branch = storage.createBranchNode( leaf.getParentRef(), 2 );

        branch.createLeaf( null, -1, storage);
        branch.createLeaf( null, -1, storage);
        return branch;
    }


    /**
     * Analyses a LeafNode and returns the most appropriate NodeManager for that node
     * @param leaf
     * @return
     */
    protected NodeManager getBestNodeManager(LeafNode leaf) {

        NodeManager bestNodeManager = null;
        float bestScore = 0.0f;

        if (splitters == null ) {
            initSplitters();
        }

        // Iterate over splitters and return best
        for (AbstractAttributeSplitter splitter : splitters.values()) {
            if (splitter != null)
            {
                Recommendation recommendation = splitter.getRecommendation( leaf, splitter.getSplitId() );
                if (recommendation != null) {
                    if (log.isDebugEnabled()) {
                        log.debug( recommendation.toString() );
                    }

                    if ( recommendation.getScore() > bestScore ){
                        bestScore = recommendation.getScore();
                        bestNodeManager = recommendation.getNodeManager();
                    }
                }
            }
        }
        if ( bestNodeManager != null){
            if (log.isDebugEnabled()) {
                log.debug( "BEST = " + bestNodeManager.toString() );
            }
        } else {
            log.info( "No NodeManager found using strategy:" + strategy.getName() );
        }
        return bestNodeManager;
    }

    /**
     * When first required, initialise a Map of splitters from the stored splitConfigurations.
     */
    private void initSplitters() {
        splitters = new TreeMap<Integer, AbstractAttributeSplitter>();

        for (SplitConfiguration splitConf : ((ManualIndexStrategy) strategy).getSplitConfigurations() ) {
            AbstractAttributeSplitter splitter = SplitterFactory.getInstance().createSplitter( splitConf );
            splitters.put( splitter.getSplitId(), splitter );
        }

    }

    public IndexStrategy getStrategy() {
        return strategy;
    }


}
