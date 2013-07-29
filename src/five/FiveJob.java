package five;

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
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;

public class FiveJob {

	public static void writeSequenceFile(String mpFsUrl) throws IOException {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(mpFsUrl), conf);

		Text value = new Text();
		Text key = new Text();

		// 文件的key必须是有序的，否则报错
		String[] srcFiles = { "D:/test/hadoop_src/2014",
				"D:/test/hadoop_src/2013" };

		int filesLen = srcFiles.length;

		MapFile.Writer writer = null;
		try {

			writer = new MapFile.Writer(conf, fs, mpFsUrl, Text.class,
					Text.class, CompressionType.RECORD);

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

	public static void convertSequenceFile2MapFile(String sfUri, String mfUri)
			throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(mfUri), conf);

		Path map = new Path(mfUri);
		Path mapData = new Path(map, MapFile.DATA_FILE_NAME);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, mapData, conf);

		Class keyClass = reader.getKeyClass();
		Class valueClass = reader.getValueClass();
		reader.close();

		long entries = MapFile.fix(fs, map, keyClass, valueClass, false, conf);

		System.out.printf("create mapfile %s with %d entries \n", map, entries);

	}

	public static void main(String[] args) throws Exception {
		// writeSequenceFile("hdfs://192.168.1.111:9000/tmp/2013_2014.mf");
		convertSequenceFile2MapFile("hdfs://192.168.1.111:9000/tmp/2013_2014.seq",
				"hdfs://192.168.1.111:9000/tmp/2013_2014.mf");
	}

}
