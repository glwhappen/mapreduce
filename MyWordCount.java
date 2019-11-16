import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyWordCount {
	public static class WCMapper 
			extends Mapper<Object, Text, Text, LongWritable> {

		protected void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] words = value.toString().split(" ");
			for(String word : words) {
				context.write(new Text(word), new LongWritable(1));
			}
		}
	}
	public static class WCReducer 
			extends Reducer<Text, LongWritable, Text, LongWritable> {

		protected void reduce(Text key, Iterable<LongWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			for(LongWritable value : values) {
				sum += value.get();
			}
			context.write(key, new LongWritable(sum));	
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://happen-pc:9000");
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(MyWordCount.class);
		job.setMapperClass(WCMapper.class);
		job.setCombinerClass(WCReducer.class);
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}


