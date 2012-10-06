/*
 * Created on 12-Nov-2004
 *
 * (C) Copyright 2004, TheOne, Adrian Clarkson & Neale Upstone.  All rights reserved.
 */
package com.wwm.db.server.wwstrategies.manual;


/**
 * A split recommendation.
 * 
 * @author Neale
 */
public class Recommendation {

    protected float _score;
    protected NodeManager _nodeManager;

    /**
     * Usual constructor
     * @param score
     * @param nodeManager
     */
    public Recommendation( float score, NodeManager nodeManager ) {
        
        _score = score;
        _nodeManager = nodeManager;
    }
    
    /**
     * @return Returns recommended NodeManager
     */
    public NodeManager getNodeManager() {
        return _nodeManager;
    }
    
    /**
     * @return Returns the _score.
     */
    public float getScore() {
        return _score;
    }
    
    
    @Override
	public String toString() {
        return "Recommendation: " + _nodeManager + ".  Score = " + _score; 
    }
}
