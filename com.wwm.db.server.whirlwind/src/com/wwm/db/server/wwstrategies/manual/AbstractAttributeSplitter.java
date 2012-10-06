/*
 * Created on 11-Nov-2004
 *
 * (C) Copyright 2004, TheOne, Adrian Clarkson & Neale Upstone.  All rights reserved.
 */
package com.wwm.db.server.wwstrategies.manual;

import java.io.Serializable;
import java.util.Collection;

import org.fuzzydb.client.marker.IWhirlwindItem;
import org.fuzzydb.core.marker.IAttributeContainer;
import org.fuzzydb.core.whirlwind.internal.IAttribute;

import com.wwm.db.server.whirlwind.internal.LeafNode;


/**
 * Interface to classes that know about a given target attribute type, and how
 * to optimally split a collection based on that attribute type. (e.g.
 * AgeSpitter would know how to split the DateOfBirthAttribute based on age, and
 * StarSignSplitter would know how to split it based on star sign ).
 *
 * @author Neale
 */
public abstract class AbstractAttributeSplitter implements Serializable {

    protected int _collectionSize;

    protected int splitAttrId;

    /**
     * Record which attrId we're splitting on
     *
     * @param splitAttrId
     * @param have
     *            boolean true if we are splitting the have, false if splitting
     *            the want
     */
    protected AbstractAttributeSplitter(int attrId) {
        this.splitAttrId = attrId;
        // TODO Could lookup selectorObject here
        // if (isHave) {
        // this.selectorClass =
        // AttributeFactory.getInstance().getHaveConstraint( splitAttrId );
        // }
        // else {
        // this.selectorClass =
        // AttributeFactory.getInstance().getWantConstraint( splitAttrId );
        // }
    }

    /**
     * @param node
     *            Collection of items
     * @param attrId
     *            Class of item to analyse
     * @return float between 0 and 1 giving relative value of splitting based on
     *         itemAttributeClass
     */
    public Recommendation getRecommendation(LeafNode node, int attrId) {
        reset();

        assert (this.splitAttrId == attrId); // A forerunner to getting rid
        // of attrId parameter

        Collection<IWhirlwindItem> items = node.getItems();
        _collectionSize = items.size();

        // Iterate over items, and call addAttribute() on relevant attribute to
        for (IAttributeContainer item : items) {

            // Split on Attr
            IAttribute a = item.getAttributeMap().findAttr(attrId);

            analyseAttribute(a); // NOTE: Always call, as null can be
            // statistically relevant

        }

        return getRecommendation();
    }

    public int getSplitId() {
        return splitAttrId;
    }

    /**
     * Reset the splitter
     *
     */
    protected abstract void reset();

    /**
     * Add attribute to rank. Subclass must implement this. e.g. For
     * AgeSplitter, this may update an average calculation, or a median (the
     * value that splits the population)
     *
     * @param attribute -
     *            The attribute to add (null is allowed)
     */
    protected abstract void analyseAttribute(IAttribute attribute);

    protected abstract Recommendation getRecommendation();

}
