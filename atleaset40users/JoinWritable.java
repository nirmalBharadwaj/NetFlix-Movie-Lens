package com.movie.atleaset40users;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class JoinWritable  implements Writable{
	
	private Text mrValue;
	private Text mrFileName;
	
	public JoinWritable() {
		set(new Text(),new Text());
	}
	
	public JoinWritable(Text mrValue,Text mrFileName) {
		set(mrValue,mrFileName);
	}

	public JoinWritable(String  mrValue,String  mrFileName) {
		set(new Text(mrValue),new Text(mrFileName));
	}

	public void set(Text mrValue,Text mrFileName) {
		this.mrValue = mrValue;
		this.mrFileName = mrFileName;
	} 
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		mrValue.readFields(in);
		mrFileName.readFields(in);
		
	}
	
	public String  getValue() {
		return mrValue.toString();
	}
	
	public String getFileName() {
		return mrFileName.toString();
	}
	

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		mrValue.write(out);
		mrFileName.write(out);
		
	}
	
	public String toString() {
		return mrValue.toString()+"\t"+mrFileName.toString();
	}

}
