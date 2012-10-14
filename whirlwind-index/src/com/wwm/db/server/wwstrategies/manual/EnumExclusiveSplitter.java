package com.wwm.db.server.wwstrategies.manual;


import org.fuzzydb.core.Settings;
import org.fuzzydb.core.whirlwind.internal.IAttribute;




/**
 * FIXME: This shouldn't need to know about EnumDefinition, as it could sort the counts out as it goes along.
 * It'd probably be notably faster to use int[] instead of ArrayList<Integer> too.
 *
 * @author ac
 *
 */
public class EnumExclusiveSplitter extends AbstractAttributeSplitter {

	private static final long serialVersionUID = 1L;
	private final int[] counts = new int[org.fuzzydb.attrs.enums.EnumDefinition.MAX_ENTRIES];
	private final int numEnumValues;
	private int attrCount;
	private final float priority;


	public EnumExclusiveSplitter(org.fuzzydb.attrs.enums.SingleEnumPriority configuration) {
		super( configuration.getId() );
		numEnumValues = configuration.getSize();
		priority = configuration.getPriority();
		reset();
	}

	/* (non-Javadoc)
	 * @see likemynds.db.indextree.AbstractAttributeSplitter#reset()
	 */
	@Override
	protected void reset() {
		for (int i=0; i < counts.length; i++) {
			counts[i] = 0;
		}
		_collectionSize = 0;
		attrCount = 0;
	}


	/* (non-Javadoc)
	 * @see likemynds.db.indextree.AbstractAttributeSplitter#analyseAttribute(com.wwm.db.core.whirlwind.internal.IAttribute)
	 */
	@Override
	protected void analyseAttribute(IAttribute attribute) {
		if (attribute == null){
			return;
		}

		short value;
        switch ( Settings.getInstance().getScorerVersion() ){
        case compact:	// fallthru
        case v2:
        	org.fuzzydb.attrs.enums.EnumExclusiveValue enumVal = (org.fuzzydb.attrs.enums.EnumExclusiveValue)attribute;
        	value = enumVal.getEnumIndex();
        	break;
        default:
        	throw new UnsupportedOperationException();
        }

		attrCount++;
		counts[value]++;
	}


	/* (non-Javadoc)
	 * @see likemynds.db.indextree.AbstractAttributeSplitter#getRecommendation()
	 */
	@Override
	protected Recommendation getRecommendation() {
		int max = 0;
		int sumSquared = 0;

		for (int i=0; i < counts.length; i++) {
			int value = counts[i];
			max = value > max ? value : max;
			sumSquared += value * value;
		}

		// TODO: Work out what this does and whether the range of the enumDef is at all relevant
		float normal = (float)Math.sqrt(max * max * (numEnumValues - 1));
		float vector = (float)Math.sqrt(sumSquared - max * max);

		float score = normal==0 ? 0f : vector/normal;

		score *= attrCount;
		score /= _collectionSize;

		assert(score >= 0f);
		assert(score <= 1f);

		return new Recommendation(score * priority, new EnumNodeManager(this.splitAttrId, numEnumValues));
	}

	@Override
	public String toString() {
		return super.toString() + ", pri = " + priority;
	}
}
