package de.tu_berlin.dima.bigdata.matrixfactorization.solve;

import java.util.Iterator;

import org.apache.mahout.math.Vector;

import de.tu_berlin.dima.bigdata.matrixfactorization.type.PactVector;
import de.tu_berlin.dima.bigdata.matrixfactorization.util.Util;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

//@Combinable
//@ConstantFields(2)
public class ItemFeatureMatrixReducer extends ReduceStub{
	
	private final PactRecord outputRecord = new PactRecord();
	private final int numItems = Util.numItems;
	
	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> collector)
			throws Exception {
		PactRecord currentRecord = null;


		
		while (records.hasNext()) {
			currentRecord = records.next();
			//itemID starts from 1
			int itemID = currentRecord.getField(2, PactInteger.class).getValue();
			Vector itemFeatureVector = currentRecord.getField(1, PactVector.class).get();
//			System.out.println("itemID:" + itemID + " Vector: " + itemFeatureVector.toString());

			PactVector result = new PactVector();
			result.set(itemFeatureVector);
			outputRecord.setField(itemID, result);
		}
		
		outputRecord.setField(0, new PactInteger(numItems));
//		System.out.println(outputRecord.getField(1680, PactVector.class).get().toString());
		collector.collect(outputRecord);
	}
	
}