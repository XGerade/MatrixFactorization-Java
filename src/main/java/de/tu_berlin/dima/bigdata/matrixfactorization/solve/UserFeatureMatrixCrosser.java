/*
 * Project: MatrixFactorization
 * @author Fangzhou Yang
 * @author Xugang Zhou
 * @version 1.0
 */

package de.tu_berlin.dima.bigdata.matrixfactorization.solve;

import java.util.List;

import org.apache.log4j.PropertyConfigurator;
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
 * This Cross class cross the user-rating-vector and the item-feature-matrix to produce a feature-vector for each user
 */
public class UserFeatureMatrixCrosser extends CrossStub{
	
	private final PactRecord outputRecord = new PactRecord();
	private final PactVector userFeatureVectorWritable = new PactVector();
	private final double lambda = Util.lambda;
	private final int numFeatures = Util.numFeatures;
	
	  /*
	   * This override method defines how the feature-vector is calculated
	   * It will find all the feature-vectors in the item-feature-matrix whose corresponding itemID gets nonZero in the user-rating-vector
	   * And calculate the feature-vector using ALS method 
	   * @param l:(userID, rating-vector)
	   * @param r:(numItems, item-feature-matrix) There would be only one item-feature-matrix in this case
	   * @return (0, user-feature-vector, userID) In the following step, all the feature-vector would be reduced to a feature-matrix
	   * So the first field of the output would be 0 for the reduce operation's convenience
	   */
	@Override
	public void cross(PactRecord userRatingVectorRecord, PactRecord itemFeatureMatrixRecord,
			Collector<PactRecord> collector) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
	    /*
	     * Get user information and user-rating-vector
	     */
		int userID = userRatingVectorRecord.getField(0, PactInteger.class).getValue();
		Vector userRatingVector = userRatingVectorRecord.getField(1, PactVector.class).get();
		
	    /*
	     * Get item-feature-matrix
	     */
		int numItems = itemFeatureMatrixRecord.getField(0, PactInteger.class).getValue();
	     /*
	     * Extract all the item-feature-vectors from the item-feature-matrix
	     * Add them to a HashMap
	     */
		OpenIntObjectHashMap<Vector> itemFeatureMatrix = numItems > 0
			        ? new OpenIntObjectHashMap<Vector>(numItems) : new OpenIntObjectHashMap<Vector>();
		for(int i = 1; i <= Util.maxItemID; i ++){
			if(!itemFeatureMatrixRecord.isNull(i)){
				Vector tmp = itemFeatureMatrixRecord.getField(i, PactVector.class).get();
				itemFeatureMatrix.put(i, tmp);
			}
		}
	    /*
	     * Add all the feature-vectors whose corresponding itemID gets nonZero in the user-rating-vector to a list
	     */
		List<Vector> featureVectors = Lists.newArrayListWithCapacity(userRatingVector.getNumNondefaultElements());
	    for (Vector.Element e : userRatingVector.nonZeroes()) {
	      int index = e.index();
	      if(itemFeatureMatrix.containsKey(index)){
	    	  featureVectors.add(itemFeatureMatrix.get(index));	  
	      }else{
	    	  System.out.println("Error! no such item:" + index +" in itemFeatureMatrix");
	      }
	    }
	    /*
	     * Calculate the feature-vector for the user using ALS
	     */
		Vector userFeatureVector = AlternatingLeastSquaresSolver.solve(featureVectors, userRatingVector, lambda, numFeatures);
		userFeatureVectorWritable.set(userFeatureVector);
		outputRecord.setField(2, new PactInteger(userID));
		outputRecord.setField(1, userFeatureVectorWritable);
		outputRecord.setField(0,new PactInteger(0));
		collector.collect(outputRecord);		
	}
	
}