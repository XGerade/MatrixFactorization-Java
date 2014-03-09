package de.tu_berlin.dima.bigdata.matrixfactorization.itemrating;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import de.tu_berlin.dima.bigdata.matrixfactorization.type.PactVector;
import de.tu_berlin.dima.bigdata.matrixfactorization.util.Util;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class ItemRatingVectorMapper extends MapStub{
	
	
    private final Vector ratings = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
    
	private final PactRecord outputRecord = new PactRecord();
	private final PactVector pactVector = new PactVector(true);
	private final PactInteger pactItemID = new PactInteger();

	@Override
	public void map(PactRecord record, Collector<PactRecord> collector)
			throws Exception {
		String text = record.getField(0, PactString.class).toString();
//		System.out.println(text);
		String[] tokens = Util.splitPrefTokens(text);
		int userID = Util.readID(tokens[Util.USER_ID_POS]);
		int itemID = Util.readID(tokens[Util.ITEM_ID_POS]);
		float rating = Util.readRate(tokens[Util.RATING_POS]);

		ratings.setQuick(userID, rating);
		pactItemID.setValue(itemID);
		pactVector.set(ratings);
		
//		System.out.println(pactItemID + " " + pactVector.vectorWritable.toString());

		outputRecord.setField(0, pactItemID);
		outputRecord.setField(1, pactVector);

		
		
		collector.collect(outputRecord);
		
		// prepare instance for reuse
		ratings.setQuick(userID, 0.0d);
	}
	
}