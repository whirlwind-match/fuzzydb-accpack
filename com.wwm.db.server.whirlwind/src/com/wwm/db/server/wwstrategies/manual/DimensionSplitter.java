/*
 * Created on 11-Nov-2004
 *
 * (C) Copyright 2004, TheOne, Adrian Clarkson & Neale Upstone.  All rights reserved.
 */
package com.wwm.db.server.wwstrategies.manual;

import org.fuzzydb.attrs.dimensions.DimensionPriority;
import org.fuzzydb.core.LogFactory;
import org.fuzzydb.core.whirlwind.internal.IAttribute;
import org.fuzzydb.dto.dimensions.IDimensions;
import org.slf4j.Logger;





/**
 * Splitter to split Node  based on Dimensions
 * @author Neale
 */
public class DimensionSplitter extends AbstractAttributeSplitter {

	private static final long serialVersionUID = 4051041973727082809L;

	private static Logger log = LogFactory.getLogger(DimensionSplitter.class);

    protected int numDimensions;
	private final IDimensions expectedRanges;
	private final IDimensions splitPriorities;

    // Array for axes, and constants to index into it.
    transient protected NumericalAnalyser analyser[] = null;

    int collectionSize = 0; // TODO: Isn't this in the base class as _collectionSize ??


    public DimensionSplitter(DimensionPriority configuration) {
    	super( configuration.getId() );
		expectedRanges = configuration.getExpected();
		splitPriorities = configuration.getPriority();
    	assert(expectedRanges.getNumDimensions() == splitPriorities.getNumDimensions());
    	numDimensions = expectedRanges.getNumDimensions();
        reset();
	}


    /*
     *  (non-Javadoc)
     * @see likemynds.db.indextree.AbstractAttributeSplitter#reset()
     */
    @Override
	protected void reset(){
        if (analyser == null) analyser = new NumericalAnalyser[numDimensions];
        for (int i = 0; i < numDimensions; i++) {
            if (splitPriorities.getDimension(i) > 0) {
            	analyser[i] = new NumericalAnalyser();
            }
        }
        collectionSize = 0;
    }


    /* (non-Javadoc)
     * @see likemynds.db.indextree.AbstractAttributeSplitter#addAttribute(likemynds.db.indextree.attributes.IAttribute)
     */
    @Override
	protected void analyseAttribute(IAttribute attribute) {

        collectionSize++;  // Always increment this, so we know how many of this attribute there are

        if (attribute == null) return; // normal

       IDimensions attr = (IDimensions) attribute; // Expect this to throw a cast exception if we mess up.

       for (int i = 0; i < numDimensions; i++) {
    	   if (splitPriorities.getDimension(i) > 0) {
    		   analyser[i].addValue( attr.getDimension(i) );
    	   }
       }
    }


    /* (non-Javadoc)
     * @see likemynds.db.indextree.AbstractAttributeSplitter#getRecommendation()
     */
    @Override
	protected Recommendation getRecommendation() {

        // The aim here is to return the axis with the highest standard deviation (to give indication of
        // spread), and with a score proportional to the number of items.
        // The recommendation will be to split on the average

        if ( collectionSize < 2) return null; // Can't split on this criteria

        if (log.isTraceEnabled()) {
            for (int i = 0; i < numDimensions; i++) {
                log.trace("Analysis (" + i + "): " + analyser[i].toString() );
            }
        }

        // Find highest SD
        double highestVal = 0;
        int indexForHighVal = 0;
        boolean hiValKnown = false;
        for (int i = 0; i < numDimensions; i++) {
        	if (splitPriorities.getDimension(i) > 0) {
	            double sd = analyser[i].getStandardDeviation() * splitPriorities.getDimension(i) / expectedRanges.getDimension(i);
	            if (sd > highestVal){
	                highestVal = sd;
	                indexForHighVal = i;
	                hiValKnown = true;
	            }
        	}
        }

        if (!hiValKnown) return null;

        NumericalAnalyser bestResult = analyser[indexForHighVal];

        if (log.isDebugEnabled()) {
            log.debug( "Best is (" + indexForHighVal + "): " + bestResult.toString() );
        }
        // normalise so we get 1 if we get expectedRange
        float rangeScore = ( bestResult.getMax() - bestResult.getMin()) / expectedRanges.getDimension(indexForHighVal);
        float score = rangeScore * bestResult.getCount() / collectionSize;

        return getDimensionRecommendation(indexForHighVal, score);
    }


    /**
     * Overridden version of super's, so that we return a FloatRangePreferenceNodeManager
     * @param indexForHighVal
     * @param score
     * @return Recommendation
     */
    protected Recommendation getDimensionRecommendation(int indexForHighVal, float score) {
        NodeManager manager = new DimensionNodeManager( splitAttrId,
                analyser[indexForHighVal].getMean(), indexForHighVal, numDimensions );

        return new Recommendation( score, manager );
    }

}
