package de.tu_berlin.dima.bigdata.matrixfactorization.itemrating;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import de.tu_berlin.dima.bigdata.matrixfactorization.type.PactVector;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class ItemAverageRatingMapper extends MapStub{
	
	private final PactInteger firstIndex = new PactInteger(0);
    private final Vector featureVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
    private final PactVector featureVectorWritable = new PactVector();
    private final PactRecord outputRecord = new PactRecord();

	@Override
	public void map(PactRecord record, Collector<PactRecord> collector)
			throws Exception {
		
		float sum = 0;
		int count = 0;
		for (Vector.Element e : record.getField(1, PactVector.class).get()
				.nonZeroes()) {
			sum += e.get();
			count ++;
		}

		int itemID = record.getField(0, PactInteger.class).getValue();
		featureVector.setQuick(itemID, sum/count);
		featureVectorWritable.set(featureVector);
		outputRecord.setField(0, firstIndex);
		outputRecord.setField(1, featureVectorWritable);

		collector.collect(outputRecord);

		// prepare instance for reuse
		featureVector.setQuick(itemID, 0.0d);
		
	}
}