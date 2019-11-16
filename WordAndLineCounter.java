// __        __            _      _              _   _     _            
// \ \      / /__  _ __ __| |    / \   _ __   __| | | |   (_)_ __   ___ 
//  \ \ /\ / / _ \| '__/ _` |   / _ \ | '_ \ / _` | | |   | | '_ \ / _ \
//   \ V  V / (_) | | | (_| |  / ___ \| | | | (_| | | |___| | | | |  __/
//    \_/\_/ \___/|_|  \__,_| /_/   \_\_| |_|\__,_| |_____|_|_| |_|\___|
//                                                                      
//   ____                  _            
//  / ___|___  _   _ _ __ | |_ ___ _ __ 
// | |   / _ \| | | | '_ \| __/ _ \ '__|
// | |__| (_) | |_| | | | | ||  __/ |   
//  \____\___/ \__,_|_| |_|\__\___|_|   
                                     
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordAndLineCounter {
	enum MyCounterWordCount {
		COUNT_WORDS, COUNT_LINES
	}

	public static class WordAndLineCounterMapper
			extends Mapper<LongWritable, Text, Text, LongWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Counter counter = context.getCounter(MyCounterWordCount.COUNT_LINES);
			counter.increment(1L);

			String[] words = value.toString().split(" ");
			for(String word : words) {
					context.getCounter(MyCounterWordCount.COUNT_WORDS).increment(1L);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(WordAndLineCounter.class);
		job.setMapperClass(WordAndLineCounterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]));
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
