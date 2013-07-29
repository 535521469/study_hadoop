package one;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class OneJob {

	public enum OneEnum {
		JOB, MAPPER, COMBINER, REDUCER, ;
	}

	public static void main(String[] args) throws Exception {
		runAsJob(args);
	}

	public static void runAsJob(String[] args) throws Exception {

		String[] inputs = { "hdfs://192.168.1.111:9000/tmp/2013",
				"hdfs://192.168.1.111:9000/tmp/2014",
				"hdfs://192.168.1.111:9000/tmp/2015" };
		String output = "hdfs://192.168.1.111:9000/tmp/one_out";

		Job job = new Job();
		job.setJarByClass(OneJob.class);

		Configuration configuration = new Configuration();
		new GenericOptionsParser(configuration, args).getRemainingArgs();

		job.setMapperClass(OneMapper.class);
		// 为了测试Combine，估MapOutputValueClass和ReduceOutputValueClass不一致
		job.setCombinerClass(OneCombine.class);

		job.setReducerClass(OneReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputValueClass(LongWritable.class);

		for (int i = 0; i < inputs.length; i++) {
			String input = inputs[i];
			FileInputFormat.addInputPath(job, new Path(input));
		}
		FileOutputFormat.setOutputPath(job, new Path(output));

		int result = job.waitForCompletion(true) ? 0 : 1;

		// 使用计数器
		System.out.println(OneEnum.MAPPER.toString() + ":"
				+ job.getCounters().findCounter(OneEnum.MAPPER).getValue());
		System.out.println(OneEnum.COMBINER.toString() + ":"
				+ job.getCounters().findCounter(OneEnum.COMBINER).getValue());
		System.out.println(OneEnum.REDUCER.toString() + ":"
				+ job.getCounters().findCounter(OneEnum.REDUCER).getValue());

		System.exit(result);
	}

}
