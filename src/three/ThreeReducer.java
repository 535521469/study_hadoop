package three;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ThreeReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Map<String, Long> map = new HashMap<String, Long>();

		for (Text text : values) {
			String value = text.toString();
			if (map.containsKey(value)) {
				map.put(value, map.get(value) + 1);
			} else {
				map.put(value, 1L);
			}
		}

		long max = 0;
		String nums = null;
		for (Entry<String, Long> entry : map.entrySet()) {
			if (max <= entry.getValue()) {
				nums = entry.getKey();
				max = entry.getValue();
			}
		}

		context.write(key, new Text(nums +">>"+ max));
//		map = new HashMap<String, Long>();
	}
}
