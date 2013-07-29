package seven;
import java.io.File;
import java.io.IOException;

import one.OneCombine;
import one.OneJob;
import one.OneMapper;
import one.OneReducer;
import one.OneJob.OneEnum;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SevenJob extends Configured implements Tool {

	static class SevenMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.getCounter(OneEnum.MAPPER).increment(1);
			String[] valuesStrings = value.toString().split(":");

			String time = valuesStrings[0];
			String[] numStrings = valuesStrings[1].split(",");

			int count = 0;
			for (String numString : numStrings) {
				if (numString.equals("9")) {
					count = count + 1;
				}
			}

			context.write(new Text(time.substring(0, 8)),
					new IntWritable(count));

		}
	}

	static class SevenReducer extends
			Reducer<Text, IntWritable, Text, LongWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			context.getCounter(OneEnum.REDUCER).increment(1);
			Long count = 0L;
			for (IntWritable intWritable : values) {
				count = count + intWritable.get();
			}

			context.write(key, new LongWritable(count));

		}

	}

	@Override
	public int run(String[] args) throws Exception {
		String[] inputs = { "hdfs://192.168.1.111:9000/tmp/2013",
				"hdfs://192.168.1.111:9000/tmp/2014",
				"hdfs://192.168.1.111:9000/tmp/2015" };
		String output = "hdfs://192.168.1.111:9000/tmp/seven_out";

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

		return result;
	}

	public static void main(String[] args) throws Exception {
		FileUtils.deleteDirectory(new File("data/output"));

		ToolRunner.run(new SevenJob(), null);
	}
}