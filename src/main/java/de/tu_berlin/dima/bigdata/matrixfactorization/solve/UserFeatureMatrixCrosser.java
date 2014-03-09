package de.tu_berlin.dima.bigdata.matrixfactorization.solve;

import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
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

public class UserFeatureMatrixCrosser extends CrossStub{
	
	private final PactRecord outputRecord = new PactRecord();
	private final PactVector userFeatureVectorWritable = new PactVector();
	private final double lambda = Util.lambda;
	private final int numFeatures = Util.numFeatures;
//	private static final Logger LOGGER = Logger.getLogger(log4jTest.class.getName()); 
	
	@Override
	public void cross(PactRecord userRatingVectorRecord, PactRecord itemFeatureMatrixRecord,
			Collector<PactRecord> collector) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		int userID = userRatingVectorRecord.getField(0, PactInteger.class).getValue();
//		LOGGER.info("UserID: " + userID);
		Vector userRatingVector = userRatingVectorRecord.getField(1, PactVector.class).get();
		
		
		int numItems = itemFeatureMatrixRecord.getField(0, PactInteger.class).getValue();
//		System.out.println("numItems: "+ numItems);
		OpenIntObjectHashMap<Vector> itemFeatureMatrix = numItems > 0
			        ? new OpenIntObjectHashMap<Vector>(numItems) : new OpenIntObjectHashMap<Vector>();
		
		for(int i = 1; i <= Util.maxItemID; i ++){
//			System.out.println(i);
			if(!itemFeatureMatrixRecord.isNull(i)){
				Vector tmp = itemFeatureMatrixRecord.getField(i, PactVector.class).get();
				itemFeatureMatrix.put(i, tmp);
			}
		}
		
		List<Vector> featureVectors = Lists.newArrayListWithCapacity(userRatingVector.getNumNondefaultElements());
	    for (Vector.Element e : userRatingVector.nonZeroes()) {
	      int index = e.index();
	      if(itemFeatureMatrix.containsKey(index)){
	    	  featureVectors.add(itemFeatureMatrix.get(index));	  
//	    	  LOGGER.info("get itemID: " + index +" in userRatingVector of User " + userID);
	      }else{
	    	  System.out.println("Error! no such item:" + index +" in itemFeatureMatrix");
//	    	  LOGGER.debug("Error! no such item:" + index +" in itemFeatureMatrix");
	      }
	    }
		
		Vector userFeatureVector = AlternatingLeastSquaresSolver.solve(featureVectors, userRatingVector, lambda, numFeatures);
		userFeatureVectorWritable.set(userFeatureVector);
		outputRecord.setField(2, new PactInteger(userID));
		outputRecord.setField(1, userFeatureVectorWritable);
		outputRecord.setField(0,new PactInteger(0));
		collector.collect(outputRecord);		
	}
	
}