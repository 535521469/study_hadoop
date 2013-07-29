package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ExampleClient {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {

		Configuration conf = new HBaseConfiguration();

		conf.set("hbase.zookeeper.quorum", "master");
//		conf.set("hbase.zookeeper.quorum","192.168.1.111");

		HBaseAdmin admin = new HBaseAdmin(conf);

		HTableDescriptor htd = new HTableDescriptor("test");
		HColumnDescriptor hcd1 = new HColumnDescriptor("cf1");

		htd.addFamily(hcd1);
		admin.createTable(htd);
		String tablename = htd.getNameAsString();

		// a put , a get , a scan

		HTable hTable = new HTable(conf, tablename);
		Put put = new Put();

		KeyValue kv = new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"),
				Bytes.toBytes("col1"), 1L, Bytes.toBytes("value1"));
		put.add(kv);
		hTable.put(put);

		// get
		Result result = hTable.get(new Get(Bytes.toBytes("row1")));
		System.out.println("get" + result.toString());
		// scan
		Scan scan = new Scan();
		ResultScanner resultScanner = hTable.getScanner(scan);

		for (Result r : resultScanner) {
			System.out.println("scan " + r.toString());
		}

		admin.disableTable(Bytes.toBytes(tablename));
		admin.deleteTable(Bytes.toBytes(tablename));

	}
}
