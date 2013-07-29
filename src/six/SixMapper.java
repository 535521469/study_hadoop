package six;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SixMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String[] valuesStrings = value.toString().split(":");

		String time = valuesStrings[0];
		String[] numStrings = valuesStrings[1].split(",");

		int count = 0;
		for (String numString : numStrings) {
			if (numString.equals("9")) {
				count = count + 1;
			}
		}

		output.collect(new Text(time.substring(0, 8)), new IntWritable(count));

	}

}
