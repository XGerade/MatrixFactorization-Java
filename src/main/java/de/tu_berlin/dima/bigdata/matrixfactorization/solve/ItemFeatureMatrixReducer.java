/*
 * Project: MatrixFactorization
 * @author Fangzhou Yang
 * @author Xugang Zhou
 * @version 1.0
 */

package de.tu_berlin.dima.bigdata.matrixfactorization.solve;

import java.util.Iterator;

import org.apache.mahout.math.Vector;

import de.tu_berlin.dima.bigdata.matrixfactorization.type.PactVector;
import de.tu_berlin.dima.bigdata.matrixfactorization.util.Util;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

/*
 * This Reduce class reduce all the item-feature-vector to a single item-feature-matrix
 */
public class ItemFeatureMatrixReducer extends ReduceStub{
	
	private final PactRecord outputRecord = new PactRecord();
	private final int numItems = Util.numItems;
	
	/*
	 * This override Method defines how the reduce operations works
	 * @param in:Iterator[(0, item-feature-vector, itemID)] Here are all item-feature-vector with its ID
	 * @return (numItems, item-feature-matrix)
	 */
	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> collector)
			throws Exception {
		PactRecord currentRecord = null;

	    /*
	     * Add all item-feature-vector to the matrix with index itemID
	     */
		while (records.hasNext()) {
			currentRecord = records.next();
			//itemID starts from 1
			int itemID = currentRecord.getField(2, PactInteger.class).getValue();
			Vector itemFeatureVector = currentRecord.getField(1, PactVector.class).get();

			PactVector result = new PactVector();
			result.set(itemFeatureVector);
			outputRecord.setField(itemID, result);
		}
		
		outputRecord.setField(0, new PactInteger(numItems));
		collector.collect(outputRecord);
	}
	
}