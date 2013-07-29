package two;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TwoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String valueString = value.toString();
		String[] valueStrings = valueString.split(":");
		String timeString = valueStrings[0];
		String numString = valueStrings[1];
		String[] numStrings = numString.split(",");
		context.write(new Text(timeString.substring(0, 8)), new IntWritable(
				Integer.valueOf(numStrings[0])));
	}
}
