package one;

import java.io.IOException;

import one.OneJob.OneEnum;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OneCombine extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		context.getCounter(OneEnum.COMBINER).increment(1);

		int count = 0;
		for (IntWritable intWritable : values) {
			count = count + intWritable.get();
		}
		context.write(key, new IntWritable(count));
	}
}
