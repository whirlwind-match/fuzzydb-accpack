package com.wwm.db.server.wwstrategies.manual;

import org.fuzzydb.attrs.AttributePriority;
import org.fuzzydb.attrs.bool.BooleanPriority;
import org.fuzzydb.attrs.dimensions.DimensionPriority;
import org.fuzzydb.attrs.enums.SingleEnumPriority;
import org.fuzzydb.attrs.simple.FloatPriority;



public class SplitterFactory {

	static private SplitterFactory instance = new SplitterFactory();

	/**
	 * Get singleton instance
	 * @return SplitterFactory
	 */
	static public SplitterFactory getInstance() {
		return instance;
	}

    /**
     * Splitters are highly specific to the database server, and therefore not something that is exposed
     * to the client.  They do need configuring, however, so we have AttributePriority objects
     * that simply provide the configuration objects to the server.
     * This method is used by the server to get a splitter, based on a given AttributePriority. 
     * TODO: When re-writing, we ought to consider whether we might want to be able to cause
     *  a different, for example, DimensionSplitter to be used, but based on the same configuration.
     *  It seems that it probably isn't worth worrying about at the moment, as we've not had to 
     *  add any more for some time.
     * @param splitConf
     * @return
     */
	public AbstractAttributeSplitter createSplitter(AttributePriority splitConf) {
		
		if ( splitConf instanceof BooleanPriority ) {
			return new BooleanSplitter( (BooleanPriority) splitConf );
		}
		
		if (splitConf instanceof FloatPriority) {
			return new FloatSplitter( (FloatPriority) splitConf );
		}

		if (splitConf instanceof DimensionPriority) {
			return new DimensionSplitter( (DimensionPriority) splitConf );
		}
		
		if (splitConf instanceof SingleEnumPriority) {
			return new EnumExclusiveSplitter( (SingleEnumPriority) splitConf );
			
		}
		
		assert( false ); // Expect to have a splitter for all types
		return null;
	}
}
