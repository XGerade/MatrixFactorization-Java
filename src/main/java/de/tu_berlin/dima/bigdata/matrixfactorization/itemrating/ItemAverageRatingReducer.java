package de.tu_berlin.dima.bigdata.matrixfactorization.itemrating;

import java.util.Iterator;

import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

import de.tu_berlin.dima.bigdata.matrixfactorization.type.PactVector;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class ItemAverageRatingReducer extends ReduceStub{

	private final PactVector result = new PactVector();
	private final PactInteger firstIndex = new PactInteger(0);
	private final PactRecord outputRecord = new PactRecord(); 
	
	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> collector)
			throws Exception {
		
		PactRecord currentRecord = null;
		Vector merge = null;
		if(records.hasNext()){
			currentRecord = records.next();
			merge = currentRecord.getField(1, PactVector.class).get();			
		}
		while (records.hasNext()) {
			currentRecord = records.next();
			PactVector v = currentRecord.getField(1, PactVector.class);
			if (v != null) {
				for (Element nonZeroElement : v.get().nonZeroes()) {
					merge.setQuick(nonZeroElement.index(), nonZeroElement.get());
				}
			}
		}
		result.set(new SequentialAccessSparseVector(merge));
		
		outputRecord.setField(0, firstIndex);
		outputRecord.setField(1, result);
		
		collector.collect(outputRecord);
		
	}
	
}