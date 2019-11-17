//   ____ ___  __  __ __  __  ___  _   _ 
//  / ___/ _ \|  \/  |  \/  |/ _ \| \ | |
// | |  | | | | |\/| | |\/| | | | |  \| |
// | |__| |_| | |  | | |  | | |_| | |\  |
//  \____\___/|_|  |_|_|  |_|\___/|_| \_|
//                                       
//  _____ ____  ___ _____ _   _ ____  
// |  ___|  _ \|_ _| ____| \ | |  _ \ 
// | |_  | |_) || ||  _| |  \| | | | |
// |  _| |  _ < | || |___| |\  | |_| |
// |_|   |_| \_\___|_____|_| \_|____/ 
                                   
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class CommonFriends {
	public static class CommonFriendsMapper1
			extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] line = value.toString().split(":");
			String person = line[0];
			String[] friend = line[1].split(",");
			for(String val : friend) {
				context.write(new Text(val), new Text(person));
			}
		}
	}
	public static class CommonFriendsReducer1
			extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text friend, Iterable<Text> persons, Context context) 
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for(Text p : persons) {
				sb.append(p).append(",");
			}
			context.write(friend, new Text(sb.toString()));
		}
	}

	public static class CommonFriendsMapper2
			extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			String friend = line[0];
			String[] person = line[1].toString().split(",");

			Arrays.sort(person);
			for(int i = 0; i < person.length - 1; i++) {
				for(int j = i + 1; j < person.length; j++) {
					context.write(new Text(person[i] + "-" + person[j]), new Text(friend));	
				}
			}
		}
	}
	public static class CommonFriendsReducer2
			extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> friend, Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for(Text f : friend) {
				sb.append(f).append(" ");
			}
			context.write(key, new Text(sb.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path tempPath = new Path("common-friends-temp");
		Path tempFile = new Path("common-friends-temp/part-r-00000");
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(CommonFriends.class);
		job1.setMapperClass(CommonFriendsMapper1.class);
		job1.setReducerClass(CommonFriendsReducer1.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		if(fs.exists(tempPath)) {
			fs.delete(tempPath, true);
		}
		FileInputFormat.addInputPath(job1, inputPath);
		FileOutputFormat.setOutputPath(job1, tempPath);
		

		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(CommonFriends.class);
		job2.setMapperClass(CommonFriendsMapper2.class);
		job2.setReducerClass(CommonFriendsReducer2.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		if(fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileInputFormat.addInputPath(job2, tempFile);
		FileOutputFormat.setOutputPath(job2, outputPath);

		ControlledJob step1Job = new ControlledJob(job1.getConfiguration());
		ControlledJob step2Job = new ControlledJob(job2.getConfiguration());
		step1Job.setJob(job1);
		step2Job.setJob(job2);

		step2Job.addDependingJob(step1Job);

		JobControl jc = new JobControl("CommonFriends");
		jc.addJob(step1Job);
		jc.addJob(step2Job);

		Thread jobThread = new Thread(jc);
		jobThread.start();

		while(!jc.allFinished()) {
			Thread.sleep(500);
		}
		System.exit(0);
	}
}
