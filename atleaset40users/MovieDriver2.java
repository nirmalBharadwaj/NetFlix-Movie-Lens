package com.movie.atleaset40users;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MovieDriver2 {
	
	public static void main(String args[]) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		
		 Configuration conf2 = new Configuration(); 
		 Job job2 = Job.getInstance(conf2,"movie data analysis");
		 job2.setJarByClass(MovieDriver.class);
		 job2.setMapperClass(MovieMapper2.class);
		 job2.setReducerClass(MovieReducer2.class);
		 
		 job2.setMapOutputKeyClass(Text.class);
		 job2.setMapOutputValueClass(LongWritable.class);
		 job2.setOutputKeyClass(LongWritable.class);
		 job2.setOutputValueClass(Text.class);
		 
		 
		 // Input and output format
		 job2.setInputFormatClass(TextInputFormat.class);
		 job2.setOutputFormatClass(TextOutputFormat.class);
		 
		 
		 FileInputFormat.addInputPath(job2, new Path(args[3]));
		 FileOutputFormat.setOutputPath(job2, new Path(args[4]));
		 
		 
		 job2.waitForCompletion(true);
	}
 
}
