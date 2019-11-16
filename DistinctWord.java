//  ____  _     _   _            _    __        __            _ 
// |  _ \(_)___| |_(_)_ __   ___| |_  \ \      / /__  _ __ __| |
// | | | | / __| __| | '_ \ / __| __|  \ \ /\ / / _ \| '__/ _` |
// | |_| | \__ \ |_| | | | | (__| |_    \ V  V / (_) | | | (_| |
// |____/|_|___/\__|_|_| |_|\___|\__|    \_/\_/ \___/|_|  \__,_|
                                                             

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistinctWord {
	public static class DistinctWordMapper 
			extends Mapper<LongWritable, Text, Text, NullWritable> {
		private Text outkey = new Text();

		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] split = value.toString().split(" ");
			for(String word : split) {
				outkey.set(word);
				context.write(outkey, NullWritable.get());
			}
		}
	}

	public static class DistinctWordReducer 
			extends Reducer<Text, NullWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> values, Context context) 
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(DistinctWord.class);
		job.setMapperClass(DistinctWordMapper.class);
		job.setReducerClass(DistinctWordReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]));
		}
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
