package six;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;

public class SixJob {

	public enum SixEnum {
		JOB, MAPPER, COMBINER, REDUCER, ;
	}

	public static void main(String[] args) throws Exception {
		runAsJobConf(args);
	}

	public static void runAsJobConf(String[] args) throws Exception {

		 String[] inputs = { "hdfs://192.168.1.111:9000/tmp/2013",
		 "hdfs://192.168.1.111:9000/tmp/2014",
		 "hdfs://192.168.1.111:9000/tmp/2015" };
		 String output = "hdfs://192.168.1.111:9000/tmp/six_out";

//		String[] inputs = { args[0], args[1], args[2] };
//		String output = args[3];

		Configuration configuration = new Configuration(true);
		new GenericOptionsParser(configuration, args).getRemainingArgs();

		JobConf jobConf = new JobConf(SixJob.class);


		jobConf.setMapperClass(SixMapper.class);
		jobConf.setCombinerClass(SixCombine.class);
		jobConf.setReducerClass(SixReducer.class);

		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(IntWritable.class);
		for (int i = 0; i < inputs.length; i++) {
			String input = inputs[i];
			FileInputFormat.addInputPath(jobConf, new Path(input));
		}
		FileOutputFormat.setOutputPath(jobConf, new Path(output));

		JobClient.runJob(jobConf);

	}
}
