/*
 * Created on 11-Nov-2004
 *
 * (C) Copyright 2004, TheOne, Adrian Clarkson & Neale Upstone.  All rights reserved.
 */
package com.wwm.db.server.wwstrategies.manual;

import org.fuzzydb.attrs.bool.BooleanSplitConfiguration;
import org.fuzzydb.attrs.bool.BooleanValue;
import org.fuzzydb.core.whirlwind.internal.IAttribute;




/**
 * Splitter to split based on BooleanValue
 *
 * FIXME need to mod to give score of zero, or null recommendation if already split by sex
 *
 * @author Neale
 */
public class BooleanSplitter extends AbstractAttributeSplitter {

	private static final long serialVersionUID = 3833464020065007664L;

	/** Numerical analyser doesn't need to be serialised */
    transient private NumericalAnalyser analyser;

    int collectionSize = 0;

	private final float priority;



    public BooleanSplitter(BooleanSplitConfiguration splitConf) {
    	super( splitConf.getId() );
        this.priority = splitConf.getPriority();
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

        // Use 1.0 for male and 0.0 for female
		float isTrue;

    	BooleanValue attr = (BooleanValue) attribute;
    	isTrue = attr.isTrue() ? 1.0f : 0.0f;

        analyser.addValue( isTrue );
    }


    /* (non-Javadoc)
     * @see likemynds.db.indextree.AbstractAttributeSplitter#getRecommendation()
     */
    @Override
	protected Recommendation getRecommendation() {

        // FIXME first need to check if we've already split on this attrId and return null if we have
        // For now, it's going to end up that way if average is less than 0.2 or > 0.8


        if ( analyser.getCount() < 2) return null; // Can't split on this criteria

        double mean = analyser.getMean();

        float score = 1.1f; // We try to force a split on sex if it looks worth it.

        // Return null if it's not worth splitting
        if ( mean < 0.2 || mean > 0.8 ) return null; // Don't split if heavily biased
        if ( analyser.getCount() / (float)collectionSize < 0.8 ) return null; // Don't split if not 80% full

        // Score based on number that had this attribute
        NodeManager manager = new BooleanNodeManager( splitAttrId );
        return new Recommendation( score * priority, manager );
    }

	@Override
	public String toString() {
		return super.toString() + ", pri = " + priority;
    }
}
