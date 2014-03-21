/*
 * Project: MatrixFactorization
 * @author Fangzhou Yang
 * @author Xugang Zhou
 * @version 1.0
 */

package de.tu_berlin.dima.bigdata.matrixfactorization.solve;

import java.util.List;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.als.AlternatingLeastSquaresSolver;
import org.apache.mahout.math.map.OpenIntObjectHashMap;

import com.google.common.collect.Lists;

import de.tu_berlin.dima.bigdata.matrixfactorization.type.PactVector;
import de.tu_berlin.dima.bigdata.matrixfactorization.util.Util;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

/*
 * This Cross class cross the item-rating-vector and the user-feature-matrix to produce a feature-vector for each item
 */
public class ItemFeatureMatrixCrosser extends CrossStub{
	
	private final PactRecord outputRecord = new PactRecord();
	private final PactVector itemFeatureVectorWritable = new PactVector();
	private final double lambda = Util.lambda;
	private final int numFeatures = Util.numFeatures;

	/*
	 * This override method defines how the feature-vector is calculated
	 * It will find all the feature-vectors in the user-feature-matrix whose corresponding userID gets nonZero in the item-rating-vector
	 * And calculate the feature-vector using ALS method 
	 * @param l:(itemID, rating-vector)
	 * @param r:(numUsers, user-feature-matrix) There would be only one user-feature-matrix in this case
	 * @return (0, item-feature-vector, itemID) In the following step, all the feature-vector would be reduced to a feature-matrix
	 * So the first field of the output would be 0 for the reduce operation's convenience
	 */
	@Override
	public void cross(PactRecord itemRatingVectorRecord, PactRecord userFeatureMatrixRecord,
			Collector<PactRecord> collector) throws Exception {
		
	    /*
	     * Get item information and item-rating-vector
	     */
		int itemID = itemRatingVectorRecord.getField(0, PactInteger.class).getValue();
		Vector itemRatingVector = itemRatingVectorRecord.getField(1, PactVector.class).get();
		
		
	    /*
	     * Get user-feature-matrix
	     */
		int numUsers = userFeatureMatrixRecord.getField(0, PactInteger.class).getValue();

        /*
         * Extract all the user-feature-vectors from the user-feature-matrix
         * Add them to a HashMap
         */
		OpenIntObjectHashMap<Vector> userFeatureMatrix = numUsers > 0
		        ? new OpenIntObjectHashMap<Vector>(numUsers) : new OpenIntObjectHashMap<Vector>();
		for(int i = 1; i <= Util.maxUserID; i ++){
			if(!userFeatureMatrixRecord.isNull(i)){
				Vector tmp = userFeatureMatrixRecord.getField(i, PactVector.class).get();
				userFeatureMatrix.put(i, tmp);
			}
		}
		
	    /*
	     * Add all the feature-vectors whose corresponding userID gets nonZero in the item-rating-vector to a list
	     */
		List<Vector> featureVectors = Lists.newArrayListWithCapacity(itemRatingVector.getNumNondefaultElements());
	    for (Vector.Element e : itemRatingVector.nonZeroes()) {
	      int index = e.index();
	      if(userFeatureMatrix.containsKey(index)){
	    	  featureVectors.add(userFeatureMatrix.get(index));	    	  
	      }
	    }
		
	    /*
	     * Calculate the feature-vector for the item using ALS
	     */
		Vector userFeatureVector = AlternatingLeastSquaresSolver.solve(featureVectors, itemRatingVector, lambda, numFeatures);
		itemFeatureVectorWritable.set(userFeatureVector);
		outputRecord.setField(2, new PactInteger(itemID));
		outputRecord.setField(1, itemFeatureVectorWritable);
		outputRecord.setField(0,new PactInteger(0));
		collector.collect(outputRecord);		
	}
	
}