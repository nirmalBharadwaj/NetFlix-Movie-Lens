package com.movie.atleaset40users;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MovieReducer2 extends Reducer<Text,LongWritable,LongWritable,Text>{
 
	 private TreeMap<Long, String> tmap2; 
	 
	 enum ReduceCounter{
			Num_Movies_Reduce,
			Num_Rating_Reduce
		}
	  
	    @Override
	    public void setup(Context context) throws IOException, 
	                                     InterruptedException 
	    { 
	        tmap2 = new TreeMap<Long, String>(); 
	    } 
	  
	    @Override
	    public void reduce(Text key, Iterable<LongWritable> values, 
	      Context context) throws IOException, InterruptedException 
	    { 
	  
	     
	        String name = key.toString(); 
	        long count = 0; 
	        
	        if((! name.trim().equals("")))
	           context.getCounter(ReduceCounter.Num_Movies_Reduce).increment(1);
	        
	        for (LongWritable val : values) 
	        { 
	        	if(val.get() >= 0.0f )
	                context.getCounter(ReduceCounter.Num_Rating_Reduce).increment(1);
	            count = val.get(); 
	        } 
	         
	        tmap2.put(count, name); 
	        
	        if (tmap2.size() > 10) 
	        { 
	            tmap2.remove(tmap2.firstKey()); 
	        } 
	    } 
	  
	    @Override
	    public void cleanup(Context context) throws IOException, 
	                                       InterruptedException 
	    { 
	  
	        for (Map.Entry<Long, String> entry : tmap2.entrySet())  
	        { 
	  
	            long count = entry.getKey(); 
	            String name = entry.getValue(); 
	            context.write(new LongWritable(count), new Text(name)); 
	        } 
	    } 
}
