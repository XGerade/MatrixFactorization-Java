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
 * This Reduce class reduce all the user-feature-vector to a single user-feature-matrix
 */
public class UserFeatureMatrixReducer extends ReduceStub{
	
	private final PactRecord outputRecord = new PactRecord();
	private final int numUsers = Util.numUsers;
	
	/*
     * This override Method defines how the reduce operations works
     * @param in:Iterator[(0, user-feature-vector, userID)] Here are all user-feature-vector with its ID
     * @return (numUsers, user-feature-matrix)
	 */
	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> collector)
			throws Exception {
		PactRecord currentRecord = null;

	    /*
	     * Add all user-feature-vector to the matrix with index userID
	     */
		while (records.hasNext()) {
			currentRecord = records.next();


			int userID = currentRecord.getField(2, PactInteger.class).getValue();
			Vector userFeatureVector = currentRecord.getField(1, PactVector.class).get();
			PactVector result = new PactVector();
			result.set(userFeatureVector);
			outputRecord.setField(userID, result);
		}
		
		outputRecord.setField(0, new PactInteger(numUsers));
		collector.collect(outputRecord);
	}
	
}