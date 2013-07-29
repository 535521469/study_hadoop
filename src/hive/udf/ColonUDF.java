package hive.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ColonUDF extends UDF {

	static final Log LOG = LogFactory.getLog(ColonUDF.class.getName());

	public ColonUDF() {
		LOG.info("in colon UDF...construct");
	}

	public String evaluate(String inText) {
		LOG.info("in colon UDF...evaluate String inText");
		String values = inText.toString();
		return values.replace(",", ":");
	}

	public String evaluate(Text inText) {
		LOG.info("in colon UDF...evaluate Text inText");
		String values = inText.toString();
		return values.replace(",", ":");
	}

}
