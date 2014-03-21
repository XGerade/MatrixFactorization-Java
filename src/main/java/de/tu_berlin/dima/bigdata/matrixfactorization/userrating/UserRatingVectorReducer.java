/*
 * Project: MatrixFactorization
 * @author Fangzhou Yang
 * @author Xugang Zhou
 * @version 1.0
 */

package de.tu_berlin.dima.bigdata.matrixfactorization.userrating;

import java.util.Iterator;

import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;

import de.tu_berlin.dima.bigdata.matrixfactorization.type.PactVector;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

/*
 * This Reduce class reduces all user-feature-vectors to a single user-feature-vector
 * Because each item has only one rating from the user, reduce operation would be adding all vectors up
 */
@Combinable
@ConstantFields(0)
public class UserRatingVectorReducer extends ReduceStub{
	
	private final PactInteger userID = new PactInteger();
	private final PactVector result = new PactVector();
	private final PactRecord outputRecord = new PactRecord();

	/*
     * This override method define how the reduce function works
	 * @param in:Iterator(userID, rating-vector) rating-vectors of the userID
	 * @return (userID, rating-vector) The user-rating-vector which contains ratings of all items from this user
	 */
	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> collector)
			throws Exception {
		PactRecord currentRecord = null;
	    /*
	     * Get the user information from the first element of the list
	     */
		Vector sum = null;
		if(records.hasNext()){
			currentRecord = records.next();
			sum = currentRecord.getField(1, PactVector.class).get();			
		}
	    /*
	     * Iterate throw all the vector and add them up
	     */
		while (records.hasNext()) {
			currentRecord = records.next();
			sum.assign(currentRecord.getField(1, PactVector.class).get(), Functions.PLUS);
		}
		result.set(new SequentialAccessSparseVector(sum));
				
		userID.setValue(currentRecord.getField(0, PactInteger.class).getValue());
		outputRecord.setField(0, userID );
		outputRecord.setField(1, result);
		collector.collect(outputRecord);
	}
	
}