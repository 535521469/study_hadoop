package six;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SixReducer extends MapReduceBase implements
		Reducer<Text, IntWritable, Text, LongWritable> {

	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, LongWritable> output, Reporter reporter)
			throws IOException {
		Long count = 0L;
		while (values.hasNext()) {
			IntWritable intWritable = values.next();
			count = count + intWritable.get();
		}
		output.collect(key, new LongWritable(count));
	}

}
