import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutput.Format;

public static class Mapper 
		extends Mapper<Object, Text, Text, IntWritable> {
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
	}
}

public static class Reducer
		extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Interable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {

	}
}

public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "Word Count");
	
	job.setJarByClass(.class);
	job.setMapperClass(.class);
	job.setCombinerClass(.class);
	job.setReducerClass(.class);
	job.setOutputKeyClass(.class);
	job.setOutputValueClass(.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	System.exit(job.waitForCompletion(true) ? 0 : 1);
	
}
