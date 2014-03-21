/*
 * Project: MatrixFactorization
 * @author Fangzhou Yang
 * @author Xugang Zhou
 * @version 1.0
 */

package de.tu_berlin.dima.bigdata.matrixfactorization.solve;

import java.util.Random;

import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;

import de.tu_berlin.dima.bigdata.matrixfactorization.type.PactVector;
import de.tu_berlin.dima.bigdata.matrixfactorization.util.Util;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

/*
 * This Map Class maps the each itemID to a random initial feature-vector for the itemID
 */
public class InitItemFeatureMatrixMapper extends MapStub{
	
    /*
     * FeatureID could start from 0
     * So the vector will be initialize as length numFeatures with cardinality at numFeatures
     */
	private final Vector features = new SequentialAccessSparseVector(Util.numFeatures, Util.numFeatures);
	 
	private final PactRecord outputRecord = new PactRecord();
	private final PactInteger itemID = new PactInteger();
	private final PactVector featureVector = new PactVector();
	private final int numFeatures = Util.numFeatures;
	private final Random random = new Random();
	/*
	 * This override method defines how the random feature-vector is generated
	 * @param in:(itemID, rating-vector)
	 * @return (0, feature-vector, itemID) In the following step, all the feature-vector would be reduced to a feature-matrix
	 * So the first field of the output would be 0 for the reduce operation's convenience
	 */
	@Override
	public void map(PactRecord record, Collector<PactRecord> collector){
		int key = record.getField(0, PactInteger.class).getValue();
		itemID.setValue(key);
	    /*
	     * Set random float for each feature
	     */
		for(int i = 0; i < numFeatures; i ++){
			features.set(i, random.nextFloat());
		}
		featureVector.set(features);
		outputRecord.setField(2, itemID);
		outputRecord.setField(1, featureVector);
		outputRecord.setField(0, new PactInteger(0));
		collector.collect(outputRecord);
		
	}
}