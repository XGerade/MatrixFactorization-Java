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
public class UserFeatureMatrixReducer extends ReduceStub{
	
	private final PactRecord outputRecord = new PactRecord();
	private final int numUsers = Util.numUsers;
	
	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> collector)
			throws Exception {
		PactRecord currentRecord = null;

		while (records.hasNext()) {
			currentRecord = records.next();

			//userID starts from 1
			int userID = currentRecord.getField(2, PactInteger.class).getValue();
			Vector userFeatureVector = currentRecord.getField(1, PactVector.class).get();
//			System.out.println("userID:" + userID + " Vector: " + userFeatureVector.toString());
			PactVector result = new PactVector();
			result.set(userFeatureVector);
			outputRecord.setField(userID, result);
		}
		
		outputRecord.setField(0, new PactInteger(numUsers));
		collector.collect(outputRecord);
	}
	
}