/*
 * Project: MatrixFactorization
 * @author Fangzhou Yang
 * @author Xugang Zhou
 * @version 1.0
 */

package de.tu_berlin.dima.bigdata.matrixfactorization.itemrating;

import java.util.Iterator;

import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

import de.tu_berlin.dima.bigdata.matrixfactorization.type.PactVector;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

/*
 * This Reduce class reduces all item-feature-vectors to a single item-feature-vector
 * Because each user has only one rating to the item, reduce operation would be adding all vectors up
 */
public class ItemAverageRatingReducer extends ReduceStub{

	private final PactVector result = new PactVector();
	private final PactInteger firstIndex = new PactInteger(0);
	private final PactRecord outputRecord = new PactRecord(); 
	
	/*
	 * This override method define how the reduce function works
	 * @param in:Iterator(itemID, rating-vector) rating-vectors of the itemID
	 * @return (itemID, rating-vector) The item-rating-vector which contains all users' ratings of this item   
	 */
	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> collector)
			throws Exception {
		
	    /*
	     * Get the item information from the first element of the list
	     */
		PactRecord currentRecord = null;
		Vector merge = null;
		if(records.hasNext()){
			currentRecord = records.next();
			merge = currentRecord.getField(1, PactVector.class).get();			
		}
	    /*
	     * Iterate throw all the vector and add them up
	     */
		while (records.hasNext()) {
			currentRecord = records.next();
			PactVector v = currentRecord.getField(1, PactVector.class);
			if (v != null) {
				for (Element nonZeroElement : v.get().nonZeroes()) {
					merge.setQuick(nonZeroElement.index(), nonZeroElement.get());
				}
			}
		}
		result.set(new SequentialAccessSparseVector(merge));
		
		outputRecord.setField(0, firstIndex);
		outputRecord.setField(1, result);
		
		collector.collect(outputRecord);
		
	}
	
}