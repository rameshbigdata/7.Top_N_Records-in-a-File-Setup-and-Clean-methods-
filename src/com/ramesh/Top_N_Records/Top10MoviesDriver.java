package com.ramesh.Top_N_Records;

 
	import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

	public class Top10MoviesDriver {

		public static void main(String[] args) throws Exception
		{
			
			
			args = new String[] { 
					"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/Setup_Cleanup/Input_Data/Movies.txt",
					"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/Setup_Cleanup/output_data/"};
					 
					/* delete the output directory before running the job */
					FileUtils.deleteDirectory(new File(args[1])); 
					 
					if (args.length != 2) {
					System.err.println("Please specify the input and output path");
					System.exit(-1);
					}
					
					System.setProperty("hadoop.home.dir","/home/hadoop/work/hadoop-3.1.2");
					
					Configuration conf = new Configuration();
			 
			// if less than two paths
			// provided will show error
			if (args.length < 2)
			{
				System.err.println("Error: please provide two paths");
				System.exit(2);
			}

			Job job = Job.getInstance(conf, "top 10");
			job.setJarByClass(Top10MoviesDriver.class);

			job.setMapperClass(Top10MoviesMapper.class);
			job.setReducerClass(Top10MoviesReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(LongWritable.class);

			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
