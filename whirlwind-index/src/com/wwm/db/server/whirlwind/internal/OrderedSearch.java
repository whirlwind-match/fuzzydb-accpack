package com.wwm.db.server.whirlwind.internal;

import java.util.ArrayList;

import org.fuzzydb.attrs.IScoreConfiguration;
import org.fuzzydb.attrs.internal.NodeScore;
import org.fuzzydb.attrs.search.SearchSpecImpl;
import org.fuzzydb.core.LogFactory;
import org.fuzzydb.server.internal.search.NextItem;
import org.fuzzydb.server.internal.search.ResultsQ;
import org.fuzzydb.server.internal.search.Search;
import org.fuzzydb.util.NanoTimer;
import org.slf4j.Logger;




/**
 * Implementation of an in-progress search, from which we want the results in order of score.
 *
 * Other possible implementations include:<br>
 * - A distributed search across a distributed DB
 * - An unordered search, where we just want results which exceed a minimum score.  This could be a useful optimisation for newsletters
 * @author ac
 */

public class OrderedSearch implements Search {

    private static Logger log = LogFactory.getLogger(OrderedSearch.class);

    private final SearchSpecImpl spec;
    private int nextSeq=0;
    private final WorkQ workQ;
    private final ResultsQ resultsQ;
    private final NodeStorageManager storage;

    private final boolean nominee;


    private static int searchCount = 0;
    private static float searchTime = 0.0f;
    private static long searchStartTime = 0;
    private static int totalResults = 0;

    /** Begin a new search - use the helper Index.NewSearch()
     * @param spec
     * @param nominee
     */
    public OrderedSearch(SearchSpecImpl spec, IScoreConfiguration config, boolean nominee, NodeStorageManager storage) {
        super();
        this.spec = spec;
        this.storage = storage;
        this.nominee = nominee;

        resultsQ = new ResultsQ(spec.getMaxNonMatches(), spec.getScoreThreshold(), spec.getTargetNumResults());

        workQ = new WorkQ( this, resultsQ, config );
        workQ.add(new NextNode(new NodeScore(), getNextSeq(), storage.getRootNode() ));	// FIXME: NodeScore

        if ( log.isInfoEnabled() ){
            log.info( "New Search: threshold = " + spec.getScoreThreshold()
                    + ", targetNumResults = " + spec.getTargetNumResults()
                    + ", searchType = " + spec.getScorerConfig() );
        }

        //		log.info( "Index dump: ");
        //		storage.getRootNode().dumpNode(System.out, 0, storage);
    }


    /* (non-Javadoc)
     * @see com.wwm.attrs.search.Search#getSpec()
     */
    @Override
	public SearchSpecImpl getSpec() {
        return spec;
    }

    /** used internally by search code
     * @return next unique sequence number
     */
    public int getNextSeq() {
        return nextSeq++;
    }

    /* (non-Javadoc)
     * @see com.wwm.attrs.search.Search#getNextResults(int)
     */
    @Override
	public ArrayList<NextItem> getNextResults(int limit)
    {
        NanoTimer timer = new NanoTimer();
        ArrayList<NextItem> results = new ArrayList<NextItem>();
        while (results.size() < limit) {

            // If the result q is empty, expand a node, if no nodes either, end search
            if  (resultsQ.isEmpty()) {
                // need to expand some nodes
                if (workQ.isEmpty()) {
                    logResults(timer, results, storage);
                    return results;	// run out of items
                }
                //				System.out.println("** Expanding node: " + workQ.best() + ", " + workQ.best().getNode() );
                workQ.expand();
            }

            // If both result q and work q have items, pop the q with the best priority
            else if ( !workQ.isEmpty()) {
                if ( resultsQ.best().compareTo( workQ.best() ) > 0) {
                    results.add( resultsQ.pop() );
                } else {
                    //					System.out.println("** Expanding node: " + workQ.best() + ", " + workQ.best().getNode() );
                    workQ.expand();
                }
            }
            else { // result q has items, work q is empty
                results.add( resultsQ.pop() );
            }
        }
        logResults(timer, results, storage);

        return results;
    }

    private void logResults(NanoTimer timer, ArrayList<NextItem> results, NodeStorageManager storage) {

        float t = timer.getMillis();

        if (searchCount == 0) {
            searchStartTime = System.currentTimeMillis();
        }

        if ( log.isDebugEnabled() && results.size() > 0 ) {
        	LeafNode leaf = (LeafNode) results.get(0).getLeaf();
        	ShowSplitsVisitor visitor = new ShowSplitsVisitor();
			leaf.accept(visitor, storage);
            log.debug( "Split order of first item: " + visitor.toString() );
        }


        // Log some info about the work done
        if ( log.isInfoEnabled() ) {
            log.info( "# results: " + results.size()
                    + ", Nodes: " + getNodesExpanded()
                    + ", Leaves: " + getLeafNodesExpanded()
                    + ", Time (ms): " + timer.getMillis()
            );
        }

        totalResults += results.size();
        searchTime += t;
        searchCount++;


        if (searchCount == 1000) {
            float avTime = searchTime / searchCount;
            float avElapsed = (float)(System.currentTimeMillis() - searchStartTime) / searchCount;
            float avResults = (float)(totalResults) / searchCount;
            log.info("====================== SEARCH STATS =============================");
            log.info("Elapsed time per search: " + avElapsed + "ms (i.e. actual rate: " + 1000 / avElapsed + " searches per sec)");
            log.info("Mean time doing search: " + avTime + "ms (i.e. potential rate: " + 1000 / avTime + " searches per sec)");
            log.info("Mean results per search: " + avResults + " (=> SearchTime per result =" + avTime / avResults + "ms)");
            log.info("Non-search time (elapsed - search): " + (avElapsed - avTime) + "ms");
            searchTime = 0.0f;
            searchCount = 0;
            totalResults = 0;
        }
    }


    /* (non-Javadoc)
     * @see com.wwm.attrs.search.Search#isMoreResults()
     */
    @Override
	public boolean isMoreResults() {
        while (resultsQ.isEmpty()) {
            if (workQ.isEmpty()) {
                return false;
            }
            workQ.expand();
        }
        return true;
    }

    private int getNodesExpanded() {
        return workQ.getNodesExpanded();
    }

    private int getLeafNodesExpanded() {
        return workQ.getLeafNodesExpanded();
    }
    @SuppressWarnings("unused")
    private int getBranchNodesExpanded() {
        return workQ.getBranchNodesExpanded();
    }

    public NodeStorageManager getNodeStorageManager() {
        return storage;
    }

    /* (non-Javadoc)
     * @see com.wwm.attrs.search.Search#isNominee()
     */
    @Override
	public boolean isNominee() {
        return nominee;
    }
}
