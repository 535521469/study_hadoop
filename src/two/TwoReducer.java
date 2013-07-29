package two;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TwoReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		Map<Integer, Long> valueCountMap = new HashMap<Integer, Long>();

		for (IntWritable intWritable : values) {
			if (valueCountMap.containsKey(intWritable.get())) {
				valueCountMap.put(intWritable.get(),
						valueCountMap.get(intWritable.get()) + 1);
			} else {
				valueCountMap.put(intWritable.get(), 1L);
			}
		}

		context.write(key, new LongWritable(valueCountMap.get(9)));

	}
}
