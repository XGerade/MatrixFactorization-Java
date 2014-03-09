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

@Combinable
@ConstantFields(0)
public class UserRatingVectorReducer extends ReduceStub{
	
	private final PactInteger userID = new PactInteger();
	private final PactVector result = new PactVector();
	private final PactRecord outputRecord = new PactRecord();

	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> collector)
			throws Exception {
		PactRecord currentRecord = null;
		Vector sum = null;
		if(records.hasNext()){
			currentRecord = records.next();
			sum = currentRecord.getField(1, PactVector.class).get();			
		}
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