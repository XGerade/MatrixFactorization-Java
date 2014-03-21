/*
 * Project: MatrixFactorization
 * @author Fangzhou Yang
 * @author Xugang Zhou
 * @version 1.0
 */

package de.tu_berlin.dima.bigdata.matrixfactorization.itemrating;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import de.tu_berlin.dima.bigdata.matrixfactorization.type.PactVector;
import de.tu_berlin.dima.bigdata.matrixfactorization.util.Util;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

/*
 * This Map class maps each user-item-rating to a item-feature-vector 
 * which contains only information of the user's rating
 */
public class ItemRatingVectorMapper extends MapStub{
	
	
    /*
     * The userID starts from 1
     * So the initialized cardinality would be set to numUsers + 1
     */
    private final Vector ratings = new RandomAccessSparseVector(Util.numUsers + 1, 1);
    
	private final PactRecord outputRecord = new PactRecord();
    /*
     * Set Vector to use float which double is not supported by stratosphere
     */
	private final PactVector pactVector = new PactVector(true);
	private final PactInteger pactItemID = new PactInteger();

	/*
	 * This override method define how the map function works
	 * @param in:(userID, itemID, rating) A rating entry
	 * @return (itemID, item-feature-vector) The item-rating-vector which contains only rating information of that user  
	 */
	@Override
	public void map(PactRecord record, Collector<PactRecord> collector)
			throws Exception {
		String text = record.getField(0, PactString.class).toString();
		String[] tokens = Util.splitPrefTokens(text);
		int userID = Util.readID(tokens[Util.USER_ID_POS]);
		int itemID = Util.readID(tokens[Util.ITEM_ID_POS]);
		float rating = Util.readRate(tokens[Util.RATING_POS]);

		ratings.setQuick(userID, rating);
		pactItemID.setValue(itemID);
		pactVector.set(ratings);
		
		outputRecord.setField(0, pactItemID);
		outputRecord.setField(1, pactVector);

		collector.collect(outputRecord);
		ratings.setQuick(userID, 0.0d);
	}
	
}