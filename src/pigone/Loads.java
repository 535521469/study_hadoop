package pigone;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class Loads extends LoadFunc {
	protected RecordReader in = null;
	private TupleFactory mTupleFactory = TupleFactory.getInstance();

	@Override
	public InputFormat getInputFormat() throws IOException {
		return new TextInputFormat();
	}

	@Override
	public Tuple getNext() throws IOException {
		return null;
	}

	@Override
	public void prepareToRead(RecordReader arg0, PigSplit arg1)
			throws IOException {
	}

	@Override
	public void setLocation(String inputPaths, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, inputPaths);
	}
	
	

}
