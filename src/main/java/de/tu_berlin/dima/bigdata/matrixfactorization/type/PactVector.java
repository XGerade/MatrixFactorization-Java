/*
 * Project: MatrixFactorization
 * @author Fangzhou Yang
 * @author Xugang Zhou
 * @version 1.0
 */

package de.tu_berlin.dima.bigdata.matrixfactorization.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import eu.stratosphere.pact.common.type.Value;


/*
 * This Pact class is a wrapper of Vector which could be passed between on stratosphere
 */
@SuppressWarnings("serial")
public class PactVector implements Value{
	
	public VectorWritable vectorWritable = new VectorWritable();
	
	public PactVector(){
		
	}
	
	/*
	 * This constructor additionally set the precision of vector
	 * which when set to true, it will use float instead of double
	 */
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