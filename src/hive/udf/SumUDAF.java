package hive.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class SumUDAF extends UDAF {

	public static class SumUDFEvaluate implements UDAFEvaluator {

		private LongWritable sum;

		@Override
		public void init() {
			this.sum = null;
		}

		public boolean iterate(Text text) {
			String value = text.toString();
			String[] values = value.split(",");
			if (this.sum == null) {
				this.sum = new LongWritable(0);
			}
			for (int i = 0; i < values.length; i++) {
				String v = values[i];
				this.sum = new LongWritable(this.sum.get()
						+ Integer.parseInt(v));
			}
			return true;
		}

		public LongWritable terminatePartial() {
			return this.sum;
		}

		public boolean merge(Text text) {
			return this.iterate(text);
		}

		public LongWritable terminate() {
			return this.sum;
		}

	}

}
