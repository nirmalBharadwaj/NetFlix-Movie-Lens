package com.movie;

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


public class MovieDriver {

	public static void main(String[] args) throws IOException, InterruptedException  {
		 
		 Configuration conf = new Configuration();
		
			
		 try {
			 Job job = Job.getInstance(conf,"movie data analysis");
			 job.setJarByClass(MovieDriver.class);
			 job.setMapperClass(MovieMapper.class);
			 job.setReducerClass(MovieReducer.class);
			 
			 job.setMapOutputKeyClass(Text.class);
			 job.setMapOutputValueClass(JoinWritable.class);
			 job.setOutputKeyClass(Text.class);
			 job.setOutputValueClass(Text.class);
			 
			 
			 // Input and output format
			 job.setInputFormatClass(TextInputFormat.class);
			 job.setOutputFormatClass(TextOutputFormat.class);
			 
			 
			// set input and output path 
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileInputFormat.addInputPath(job, new Path(args[2]));
			FileOutputFormat.setOutputPath(job, new Path(args[3]));
	
		    boolean waitMapperReduce1 = job.waitForCompletion(true);
		    
		    if(waitMapperReduce1) {
		    	 Configuration conf2 = new Configuration();
		    	 
		    	 try {
		    		 
		    		 Job job2 = Job.getInstance(conf2,"top 10 movies");
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
		    	 catch(Exception e) {
		    		 System.out.print("Failure: "+e.getMessage());
		    	 }
		    }
			 
			 
		 }
		 catch(Exception e) {
			 System.out.print("Failure: "+e.getMessage());
		 }
			
			
		 
		
	}
}
