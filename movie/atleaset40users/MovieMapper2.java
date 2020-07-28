package com.movie.atleaset40users;

import java.io.*;
import java.util.*;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMapper2 extends Mapper<LongWritable,Text,Text,LongWritable> {
	
	
	private TreeMap<Long, String> tmap; 
	
	enum MapCounter{
		Num_Movies,
		Num_Rating
	}
	  
    @Override
    public void setup(Context context) throws IOException, 
                                     InterruptedException 
    { 
        tmap = new TreeMap<Long, String>(); 
    } 
  
    @Override
    public void map(LongWritable key, Text value, 
       Context context) throws IOException,  
                      InterruptedException 
    { 
  
       
        String[] tokens = value.toString().split("\t"); 
        String movie_name = tokens[0]; 
        
        long no_of_views = Long.parseLong(tokens[1]); 
        
        if((! movie_name.trim().equals("")))
            context.getCounter(MapCounter.Num_Movies).increment(1);
        
        if(no_of_views > 0.0f )
            context.getCounter(MapCounter.Num_Rating).increment(1);

        tmap.put(no_of_views, movie_name); 

        if (tmap.size() > 10) 
        { 
            tmap.remove(tmap.firstKey()); 
        } 
    } 
  
    @Override
    public void cleanup(Context context) throws IOException, 
                                       InterruptedException 
    { 
        for (Map.Entry<Long, String> entry : tmap.entrySet())  
        { 
  
            long count = entry.getKey(); 
            String name = entry.getValue(); 
  
            context.write(new Text(name), new LongWritable(count)); 
        } 
    }
	 
	 
}
