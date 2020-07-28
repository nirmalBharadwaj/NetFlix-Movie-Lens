package com.movie;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class MovieMapper  extends Mapper<LongWritable,Text,Text,JoinWritable>{
	 
	String filename;
	
	 public void setup(Context context) {
		 FileSplit fileSplit = (FileSplit)context.getInputSplit();
		 filename = fileSplit.getPath().getName();
		 
	 }
	 public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
		 
		 String line;
		 String[] tokens;
		 Text keyOut,valueOut;
		 if(filename.equals("movies.dat")) {
			 
			 line = value.toString();
			 tokens = line.split("::");
			 String movieId;
			 String title;
			 String genres;
			 
			 
			 if(tokens.length==3) {
				 movieId = tokens[0];
				 title=tokens[1];
				 genres=tokens[2];
				 
				 keyOut = new Text(movieId);
				 valueOut=new Text(title);
				 
				 
				 context.write(keyOut, new JoinWritable(valueOut,new Text(filename)));
			 }
			 
			
			 
		 }
		 
		 else if(filename.equals("ratings.dat")) {
			 line = value.toString();
			 tokens = line.split("::");
			 
			 String movieId;
			 String rating;
			 
			 if(tokens.length==4) {
				 movieId = tokens[1];
                 rating=tokens[2];
				 
				 keyOut = new Text(movieId);
				 valueOut=new Text(rating);
				 
				 
				 context.write(keyOut, new JoinWritable(valueOut,new Text(filename)));
			 }
			 
			 
		 }
		 /*
		 else if (filename.equals("users")) {
			 line = value.toString();
			 tokens = line.split("::");
			 
		 }
		 */
		
	}

}
