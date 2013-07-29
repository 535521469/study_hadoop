package three;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ThreeJob {

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		String[] inputs = { "hdfs://192.168.1.111:9000/tmp/2013",
				"hdfs://192.168.1.111:9000/tmp/2014",
				"hdfs://192.168.1.111:9000/tmp/2015" };
		String output = "hdfs://192.168.1.111:9000/tmp/three_out";

		Configuration configuration = new Configuration();
		new GenericOptionsParser(configuration, args).getRemainingArgs();
		
		Job job = new Job();
		job.setJarByClass(ThreeJob.class);

		job.setMapperClass(ThreeMapper.class);
		job.setReducerClass(ThreeReducer.class);

		job.setMapOutputKeyClass(Text.class);

		for (int i = 0; i < inputs.length; i++) {
			String input = inputs[i];
			FileInputFormat.addInputPath(job, new Path(input));
		}
		FileOutputFormat.setOutputPath(job, new Path(output));

		// FileOutputFormat.setCompressOutput(job, true);
		// FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
