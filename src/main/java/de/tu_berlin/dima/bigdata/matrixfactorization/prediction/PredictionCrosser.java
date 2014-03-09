package de.tu_berlin.dima.bigdata.matrixfactorization.prediction;

import org.apache.mahout.math.Vector;

import de.tu_berlin.dima.bigdata.matrixfactorization.type.PactVector;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactFloat;
import eu.stratosphere.pact.common.type.base.PactInteger;


public class PredictionCrosser extends CrossStub{
	
	private final PactRecord outputRecord = new PactRecord();

	@Override
	public void cross( PactRecord itemFeatureVectorRecord, PactRecord userFeatureVectorRecord,
			Collector<PactRecord> collector) throws Exception {
		
		int userID = userFeatureVectorRecord.getField(2, PactInteger.class).getValue();
		int itemID = itemFeatureVectorRecord.getField(2, PactInteger.class).getValue();
		
		Vector userFeatureVector = userFeatureVectorRecord.getField(1, PactVector.class).get();
		Vector itemFeatureVector = itemFeatureVectorRecord.getField(1, PactVector.class).get();
		
		float predict = (float)userFeatureVector.dot(itemFeatureVector);
		
		outputRecord.setField(0, new PactInteger(userID));
		outputRecord.setField(1, new PactInteger(itemID));
		outputRecord.setField(2, new PactFloat(predict));
		
		collector.collect(outputRecord);
		
	}
	
}