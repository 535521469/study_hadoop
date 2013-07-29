package three;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThreeMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] valuesStrings = value.toString().split(":");
		String time = valuesStrings[0];
		String numString = valuesStrings[1];

		context.write(new Text(time.substring(0, 8)),
				new Text(numString.substring(0, 5)));
	}

}
