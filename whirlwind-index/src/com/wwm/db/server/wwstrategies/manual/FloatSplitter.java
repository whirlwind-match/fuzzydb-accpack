/*
 *
 * (C) Copyright 2004, TheOne, Adrian Clarkson & Neale Upstone.  All rights reserved.
 */
package com.wwm.db.server.wwstrategies.manual;

import org.fuzzydb.attrs.simple.FloatPriority;
import org.fuzzydb.attrs.simple.FloatValue;
import org.fuzzydb.core.LogFactory;
import org.fuzzydb.core.whirlwind.internal.IAttribute;
import org.slf4j.Logger;





/**
 * Splitter to split a range of floats at their average, scored according to an expected range.
 * @author Neale
 */
public class FloatSplitter extends AbstractAttributeSplitter {

    /**
	 *
	 */
	private static final long serialVersionUID = 3256720701762517298L;

	private static Logger log = LogFactory.getLogger(FloatSplitter.class);

    transient private NumericalAnalyser analyser;

    int collectionSize = 0;
    float expectedRange = 1.0f;  		// Range that would give score of 1.0

	private final float priority;


    public FloatSplitter(FloatPriority configuration) {
    	super( configuration.getId() );
		expectedRange = configuration.getExpected();
		priority = configuration.getPriority();
        reset();
	}


	/*
     *  (non-Javadoc)
     * @see likemynds.db.indextree.AbstractAttributeSplitter#reset()
     */
    @Override
	protected void reset(){
        analyser = new NumericalAnalyser();
        collectionSize = 0;
    }


    /* (non-Javadoc)
     * @see likemynds.db.indextree.AbstractAttributeSplitter#addAttribute(likemynds.db.indextree.attributes.IAttribute)
     */
    @Override
	protected void analyseAttribute(IAttribute attribute) {

        collectionSize++;  // Always increment this, so we know how many of this attribute there are

        if (attribute == null) return; // normal

        float value = 0f;
        FloatValue dobAttr = (FloatValue) attribute;
        value = dobAttr.getValue();
        analyser.addValue( value );
    }


    /* (non-Javadoc)
     * @see likemynds.db.indextree.AbstractAttributeSplitter#getRecommendation()
     */
    @Override
	protected Recommendation getRecommendation() {

        // Convert milliseconds to years

        if ( analyser.getCount() < 2) return null; // Can't split on this criteria

        if (log.isDebugEnabled()) {
        	log.debug( "Analysis: " + analyser.toString(1 / expectedRange) );
        }

        // Derive a score based on the range as a proportion of 60 years
        float valueRange = analyser.getMax() - analyser.getMin();
        float score = valueRange / expectedRange; // normalise so we get 1 if we get expectedRange

        // Score based on number that had this attribute, and recommend average date
        float splitValue = analyser.getMean();
        NodeManager manager = new FloatNodeManager( splitAttrId, splitValue );
        return new Recommendation( priority * score * analyser.getCount() / collectionSize, manager );
    }

}
