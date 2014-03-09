package de.tu_berlin.dima.bigdata.matrixfactorization.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import eu.stratosphere.pact.common.type.Value;


/**
 * Wrapping Class, which wraps the class VectorWritable of the package mahout.math
 * @author titicaca
 *
 */
@SuppressWarnings("serial")
public class PactVector implements Value{
	
	public VectorWritable vectorWritable = new VectorWritable();
	
	public PactVector(){
		
	}
	
	public PactVector(boolean writesLaxPrecision){
		vectorWritable.setWritesLaxPrecision(writesLaxPrecision);
	}
	
	public void set(Vector v){
		vectorWritable.set(v);
	}
	
	public Vector get(){
		return vectorWritable.get();
	}

	@Override
	public void read(DataInput in) throws IOException {
		vectorWritable.readFields(in);	
	}

	@Override
	public void write(DataOutput out) throws IOException {
		vectorWritable.write(out);		
	}
	
	@Override
	public String toString(){
		return vectorWritable.toString();
	}
	
}