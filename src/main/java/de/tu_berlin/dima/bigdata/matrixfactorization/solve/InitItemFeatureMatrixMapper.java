package de.tu_berlin.dima.bigdata.matrixfactorization.solve;

import java.util.Random;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;

import de.tu_berlin.dima.bigdata.matrixfactorization.type.PactVector;
import de.tu_berlin.dima.bigdata.matrixfactorization.util.Util;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;


/**
 * input Item Rating Vectors :
 * 	key: ItemID
 *  value: Vector { userid : rating} 
 *  
 * output Initial Item Feature Matrix
 * 	key: ItemID
 * 	value: Vector { featureID : value}
 * @author titicaca
 *
 */
public class InitItemFeatureMatrixMapper extends MapStub{
	
	private final Vector features = new SequentialAccessSparseVector(Integer.MAX_VALUE, Util.numFeatures);
	 
	private final PactRecord outputRecord = new PactRecord();
	private final PactInteger itemID = new PactInteger();
	private final PactVector featureVector = new PactVector();
	private final int numFeatures = Util.numFeatures;
	private final Random random = new Random();
	@Override
	public void map(PactRecord record, Collector<PactRecord> collector){
		int key = record.getField(0, PactInteger.class).getValue();
		itemID.setValue(key);
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