package four;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.util.ReflectionUtils;

public class FourJob {

	public static void readSequenceFile(String file) throws IOException {
		String uri = file;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(
					reader.getValueClass(), conf);
			long position = reader.getPosition();
			while (reader.next(key, value)) {
				String syncSeen = reader.syncSeen() ? "*" : "";
				System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key,
						value);
				position = reader.getPosition(); // beginning of next record
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}

	public static void writeSequenceFile(String seqFsUrl) throws IOException {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(seqFsUrl), conf);
		Path seqPath = new Path(seqFsUrl);

		Text value = new Text();
		Text key = new Text();

		String[] srcFiles = { "D:/test/hadoop_src/2013",
				"D:/test/hadoop_src/2014" };

		int filesLen = srcFiles.length;

		SequenceFile.Writer writer = null;
		try {
			// enable compress
			writer = SequenceFile.createWriter(fs, conf, seqPath,
					key.getClass(), value.getClass(), CompressionType.RECORD);
			while (filesLen > 0) {
				String fileName = srcFiles[filesLen - 1];
				File gzFile = new File(fileName);

				InputStream in = new BufferedInputStream(new FileInputStream(
						gzFile));
				long len = gzFile.length();
				byte[] buff = new byte[(int) len];
				if ((len = in.read(buff)) != -1) {
					value.set(buff);
					writer.append(new Text(fileName), value);
				}

				key.clear();
				value.clear();

				IOUtils.closeStream(in);

				filesLen--;
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(writer);
		}

	}

	public static void main(String[] args) throws IOException {
		 writeSequenceFile("hdfs://192.168.1.111:9000/tmp/2013_2014.seq");
//		readSequenceFile("hdfs://192.168.1.111:9000/tmp/2013_2014.seq");
	}
}
