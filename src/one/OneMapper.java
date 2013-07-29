package one;

import java.io.IOException;

import one.OneJob.OneEnum;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OneMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

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

		context.write(new Text(time.substring(0, 8)), new IntWritable(count));

	}
}
