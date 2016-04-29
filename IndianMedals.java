package Ind;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndianMedals {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
				
				
				String line = value.toString();
				String country = "India";
				
				//String[] keyvalue = line.split("\\t|\\s+");
				String[] keyvalue = line.split("\t"); //line.split(",");
				if (keyvalue[2].equals(country))
				context.write(new Text(keyvalue[3]), new IntWritable(Integer.parseInt(keyvalue[9])));
//				else
//					context.write(new Text(keyvalue[5]), new IntWritable(Integer.parseInt(keyvalue[9])));

				}
	

		
	}
public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> value, Context con) throws IOException, InterruptedException
		{
		  int sum=0;
		  
		  for (IntWritable v:value)
		    sum +=v.get();
		  result.set(sum);
		 // System.out.println("key"+key+" sum"+ sum);
		  con.write(key, result);
		
		}

	}
	/**
	 * @param args
	 */
public static void main(String[] args)  throws Exception {
	// TODO Auto-generated method stub
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "India");
	job.setJarByClass(IndianMedals.class);
	job.setMapperClass(MyMapper.class);
	job.setCombinerClass(MyReducer.class);
	job.setReducerClass(MyReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}

}
