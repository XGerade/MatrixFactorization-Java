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


public class ItemFeatureMatrixCrosser extends CrossStub{
	
	private final PactRecord outputRecord = new PactRecord();
	private final PactVector itemFeatureVectorWritable = new PactVector();
	private final double lambda = Util.lambda;
	private final int numFeatures = Util.numFeatures;

	@Override
	public void cross(PactRecord itemRatingVectorRecord, PactRecord userFeatureMatrixRecord,
			Collector<PactRecord> collector) throws Exception {
		
		int itemID = itemRatingVectorRecord.getField(0, PactInteger.class).getValue();
		Vector itemRatingVector = itemRatingVectorRecord.getField(1, PactVector.class).get();
		
		
		int numUsers = userFeatureMatrixRecord.getField(0, PactInteger.class).getValue();
//		System.out.println("numItems:" + numItems);

		OpenIntObjectHashMap<Vector> userFeatureMatrix = numUsers > 0
			        ? new OpenIntObjectHashMap<Vector>(numUsers) : new OpenIntObjectHashMap<Vector>();
		
		for(int i = 1; i <= Util.maxUserID; i ++){
//			System.out.println(i);
			if(!userFeatureMatrixRecord.isNull(i)){
				Vector tmp = userFeatureMatrixRecord.getField(i, PactVector.class).get();
//				System.out.println(tmp.toString());
				userFeatureMatrix.put(i, tmp);
			}
//			System.out.println("size:" + itemFeatureMatrix.size());
//			System.out.println("numFields:" +userFeatureMatrixRecord.getNumFields());
		}
		
		List<Vector> featureVectors = Lists.newArrayListWithCapacity(itemRatingVector.getNumNondefaultElements());
	    for (Vector.Element e : itemRatingVector.nonZeroes()) {
	      int index = e.index();
	      if(userFeatureMatrix.containsKey(index)){
	    	  featureVectors.add(userFeatureMatrix.get(index));	    	  
	      }
	    }
		
		Vector userFeatureVector = AlternatingLeastSquaresSolver.solve(featureVectors, itemRatingVector, lambda, numFeatures);
		itemFeatureVectorWritable.set(userFeatureVector);
		outputRecord.setField(2, new PactInteger(itemID));
		outputRecord.setField(1, itemFeatureVectorWritable);
		outputRecord.setField(0,new PactInteger(0));
		collector.collect(outputRecord);		
	}
	
}