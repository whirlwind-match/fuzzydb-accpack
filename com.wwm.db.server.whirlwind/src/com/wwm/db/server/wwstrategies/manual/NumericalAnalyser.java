/*
 * Created on 17-Nov-2004
 *
 * (C) Copyright 2004, TheOne, Adrian Clarkson & Neale Upstone.  All rights reserved.
 */
package com.wwm.db.server.wwstrategies.manual;

/**
 * @author Neale
 */
public class NumericalAnalyser {
    
	private int count = 0;
    private float total = 0;
    private float totalSquared = 0;
    
    private float min = Float.MAX_VALUE;	// Start off at wrong ends so we get first value
    private float max = -Float.MAX_VALUE;

    /**
     * Update internal information TreeMap
     * @param val
     */
    public void addValue( float val ) {
        count++;
        total += val;
        totalSquared += val * val;
        min = Math.min( min, val );
        max = Math.max( max, val );
    }


    /**
     * @return number of haves that were analysed
     */
    public int getCount() {
        return count;
    }

    /**
     * @return Returns the max.
     */
    public float getMax() {
        return max;
    }
    
    /**
     * @return Returns the min.
     */
    public float getMin() {
        return min;
    }
    
    /**
     * @return double - mean of haves analysed
     */
    public float getMean() {
        return total / count;
    }
    

    /**
     * @return long - average rounded to nearest integer
     */
    public long getMeanAsLong() {
        
        return Math.round( getMean() );
    }
    
    /**
     * Get Standard Deviation as defined at http://mathworld.wolfram.com/StandardDeviation.html
     * @return
     */
    public float getStandardDeviation() {
        
        float meanSquared = getMean() * getMean();
        float rawMoment = totalSquared / count;
        
        return (float) Math.sqrt( rawMoment - meanSquared );
    }
    
    
    @Override
	public String toString(){
        return toString( 1.0f );
    }
    
    
    public String toString( float scaling ){
        return "[" + min * scaling + " - " + max * scaling + "], ave = " 
        		+ getMean() * scaling + ", sd = " + getStandardDeviation() * scaling ;
    }
    
}
