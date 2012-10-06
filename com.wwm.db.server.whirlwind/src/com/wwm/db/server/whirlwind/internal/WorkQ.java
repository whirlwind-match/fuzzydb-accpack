package com.wwm.db.server.whirlwind.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.fuzzydb.attrs.IScoreConfiguration;
import org.fuzzydb.attrs.internal.IConstraintMap;
import org.fuzzydb.attrs.internal.NodeScore;
import org.fuzzydb.attrs.search.SearchSpecImpl;
import org.fuzzydb.client.internal.MetaObject;
import org.fuzzydb.client.marker.IWhirlwindItem;
import org.fuzzydb.core.WorkManager;
import org.fuzzydb.core.whirlwind.SearchSpec.SearchMode;
import org.fuzzydb.core.whirlwind.internal.IAttribute;
import org.fuzzydb.core.whirlwind.internal.IAttributeMap;
import org.fuzzydb.server.internal.search.NextItem;
import org.fuzzydb.server.internal.search.Q;
import org.fuzzydb.server.internal.search.ResultsQ;




/**
 * Container for unexpanded nodes. Ordered by priority
 * scoreThreshold and targetNumResults are hints that allow the work queue to do less work.
 *
 * The algorithm for target num results is as follows:
 *
 * func addToQueue( item, score ) {
 * 	if
 */
public class WorkQ extends Q<NextNode> {

    private final OrderedSearch search;
    private final ResultsQ resultsQ;
    private final IScoreConfiguration scoreConfig; // Score config for this search
    private int nodesExpanded;
    private int branchNodesExpanded;
    private int leafNodesExpanded;


    /**
     *
     * @param search
     * @param resultsQ
     * @param config
     * @param scoreThreshold - scores of this value and below are discarded (0f gives all non-zero scores)
     * @param targetNumResults - number of results from this search that must be accurate (beyond this, not all
     *  						results are guaranteed to be shown, but scores will be correct)
     */
    public WorkQ(OrderedSearch search, ResultsQ resultsQ, IScoreConfiguration config) {
        super();
        this.search = search;
        this.resultsQ = resultsQ;
        this.scoreConfig = config;
    }


    /**
     * Expand the next node in this WorkQ.
     * This may result in no items being added to either workQ or resultsQ as it might be lower than scoreThreshold
     *
     */
    public void expand()
    {
        nodesExpanded++;
        NextNode nextNode = pop();

        // If the score of this node is lower than our threshold (which gets higher over the course of a limited results search)
        // then discard it, and let calling function check next best
        if ( nextNode.getScore().compareTo( resultsQ.getCurrentScoreThreshold() ) < 0 ) {
            return;
        }

        Node<?> node = nextNode.getNode();

        if ( node instanceof BranchNode ) {
            branchNodesExpanded++;
            expandBranchNode( nextNode.getScore(), (BranchNode)node );
        }
        else {
            assert(node instanceof LeafNode); // it must be a LeafNode
            leafNodesExpanded++;
            expandLeafNode( nextNode.getScore(), (LeafNode)node );
        }
    }


    private void expandLeafNode(final NodeScore currentScore, final LeafNode leaf) {
        final SearchSpecImpl searchSpec = search.getSpec();
        Collection<IWhirlwindItem> items = leaf.getItems();

        ArrayList<Future<NextItem>> results = new ArrayList<Future<NextItem>>(items.size());

        // score each item and add to results if higher than our score threshold.
        for (final IWhirlwindItem dbItem : items) {
            Callable<NextItem> task = new Callable<NextItem>() {
                @Override
				public NextItem call() throws Exception {
                    return expandItem(currentScore, leaf, searchSpec, dbItem);
                }
            };
            results.add(WorkManager.getInstance().submit(task));
        }

        for (Future<NextItem> future : results) {
            NextItem result;
            try {
                result = future.get();
                if (result != null) {
                    resultsQ.add(result);
                }
            }
            catch (InterruptedException e) {
                WorkManager.handleException(e);
            }
            catch (ExecutionException e) {
                WorkManager.handleException(e);
            }
        }
    }


    protected NextItem expandItem(final NodeScore currentScore, LeafNode leaf, SearchSpecImpl searchSpec,
            IWhirlwindItem dbItem) {
        NodeScore itemScore = (NodeScore) scoreConfig.scoreAllItemToItem(searchSpec.getAttributeMap(), dbItem.getAttributeMap(), searchSpec.getSearchMode());
        validateDecreasingScores(currentScore, itemScore);

        NextItem newItem = null;
        if (itemScore.compareTo(resultsQ.getCurrentScoreThreshold()) > 0 ) { // By default, zero, so we add all non-zero scores

            MetaObject<IWhirlwindItem> mo = new MetaObject<IWhirlwindItem>(null, -1, dbItem);  // TODO FIXME: This is a big frig!!
            newItem = new NextItem(itemScore, search.getNextSeq(), mo, leaf);
        }
        return newItem;
    }




    private void expandBranchNode(NodeScore currentScore, BranchNode bn) {
        //System.out.println("... node has " + bn.countItems() + "children");

        for (int i = 0; i < bn.getNumChildren(); i++) {
            Branch branch = bn.getNode(i);
            NodeScore childScore = new NodeScore();

            SearchSpecImpl searchSpec = search.getSpec();
            IConstraintMap nodeAttributes = branch.getAnnotations();
            SearchMode mode = searchSpec.getSearchMode();
            IAttributeMap<IAttribute> searchAttrs = searchSpec.getAttributeMap();
            // branch.score(newScore, search.getSpec(), scoreConfig);
            scoreConfig.scoreSearchToNodeBothWays(childScore, nodeAttributes, mode, searchAttrs);
            validateDecreasingScores(currentScore, childScore);
            //System.out.println("... child  has score " + score);

            // Add node if new is better than threshold
            if (childScore.compareTo(resultsQ.getCurrentScoreThreshold()) > 0 ) {
                NextNode newNode = new NextNode(childScore, search.getNextSeq(), branch.getChild(search.getNodeStorageManager()));
                add(newNode);
            }
        }
    }

    /**
     * If enabled, ensure that it is always the case that parent nodes always score >= child nodes.
     */
    private void validateDecreasingScores(NodeScore parentScore, NodeScore childScore) {
        if (/*validate*/ true){
            if ( ! childScore.allScoresLowerThan(parentScore) ){
                throw new Error("Someone got the tricky bit wrong - parent score should be >= child score ");
            }
        }
    }

    public int getNodesExpanded() {
        return nodesExpanded;
    }
    public int getBranchNodesExpanded() {
        return branchNodesExpanded;
    }
    public int getLeafNodesExpanded() {
        return leafNodesExpanded;
    }
}
