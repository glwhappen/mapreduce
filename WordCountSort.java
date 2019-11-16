// __        __            _  ____                  _   
// \ \      / /__  _ __ __| |/ ___|___  _   _ _ __ | |_ 
//  \ \ /\ / / _ \| '__/ _` | |   / _ \| | | | '_ \| __|
//   \ V  V / (_) | | | (_| | |__| (_) | |_| | | | | |_ 
//    \_/\_/ \___/|_|  \__,_|\____\___/ \__,_|_| |_|\__|
//  ____             _   
// / ___|  ___  _ __| |_ 
// \___ \ / _ \| '__| __|
//  ___) | (_) | |  | |_ 
// |____/ \___/|_|   \__|
                      
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountSort {
	public static class TokenizerMapper
			extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " ,.\":\t\n");
			while(itr.hasMoreTokens()) {
				word.set(itr.nextToken().toLowerCase());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer
			extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	private static class WcComparator
			extends IntWritable.Comparator {
		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);
		}
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path tempDir = new Path("wordcount-temp-output");

		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCountSort.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, tempDir);

		job.waitForCompletion(true);

		Job sortjob = new Job(conf, "sort");
		FileInputFormat.addInputPath(sortjob, tempDir);

		sortjob.setInputFormatClass(SequenceFileInputFormat.class);
		sortjob.setMapperClass(InverseMapper.class);
		sortjob.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(sortjob, new Path(args[1]));

		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}
		sortjob.setOutputKeyClass(IntWritable.class);
		sortjob.setOutputValueClass(Text.class);
		sortjob.setSortComparatorClass(WcComparator.class);

		sortjob.waitForCompletion(true);

		FileSystem.get(conf).delete(tempDir);
		System.exit(0);
	}
}
