package com.wwm.db.server.wwstrategies.manual;

import org.fuzzydb.attrs.SplitConfiguration;
import org.fuzzydb.attrs.bool.BooleanSplitConfiguration;
import org.fuzzydb.attrs.dimensions.DimensionSplitConfiguration;
import org.fuzzydb.attrs.enums.EnumExclusiveSplitConfiguration;
import org.fuzzydb.attrs.simple.FloatSplitConfiguration;



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
     * to the client.  They do need configuring, however, so we have SplitConfiguration objects
     * that simply provide the configuration objects to the server.
     * This method is used by the server to get a splitter, based on a given SplitConfiguration. 
     * TODO: When re-writing, we ought to consider whether we might want to be able to cause
     *  a different, for example, DimensionSplitter to be used, but based on the same configuration.
     *  It seems that it probably isn't worth worrying about at the moment, as we've not had to 
     *  add any more for some time.
     * @param splitConf
     * @return
     */
	public AbstractAttributeSplitter createSplitter(SplitConfiguration splitConf) {
		
		if ( splitConf instanceof BooleanSplitConfiguration ) {
			return new BooleanSplitter( (BooleanSplitConfiguration) splitConf );
		}
		
		if (splitConf instanceof FloatSplitConfiguration) {
			return new FloatSplitter( (FloatSplitConfiguration) splitConf );
		}

		if (splitConf instanceof DimensionSplitConfiguration) {
			return new DimensionSplitter( (DimensionSplitConfiguration) splitConf );
		}
		
		if (splitConf instanceof EnumExclusiveSplitConfiguration) {
			return new EnumExclusiveSplitter( (EnumExclusiveSplitConfiguration) splitConf );
			
		}
		
		assert( false ); // Expect to have a splitter for all types
		return null;
	}
}
