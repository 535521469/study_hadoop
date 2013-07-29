package one;

import java.io.IOException;

import one.OneJob.OneEnum;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OneReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

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
