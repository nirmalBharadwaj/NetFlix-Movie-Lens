package com.movie;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class MovieReducer extends Reducer<Text,JoinWritable,Text,Text> {

	 public void reduce(Text key, Iterable <JoinWritable> values, Context context) throws IOException, InterruptedException {
		 

		 String filename;
		 String  mapOut;
		 Text movie_title= new Text("");
		 Long total_rating=0l;
		 Text final_rating;
		 
		 for(JoinWritable value: values) {
			 
			 filename = value.getFileName();
			 mapOut = value.getValue();
			 
			 if(filename.equals("movies.dat")) {
				 String title = mapOut;
				 
				 if(title != null && ! title.trim().equals("")){
					 movie_title.set(title);
				 }
			 }
			 
			 else if(filename.equals("ratings.dat")) {
				 Long rating  = Long.parseLong(mapOut);
				 total_rating += rating;
				 
			 }
		 }
		 
		 final_rating = new Text();
		 final_rating.set(String.valueOf(total_rating) );
		 
		 context.write(movie_title,final_rating );
	 }
}
