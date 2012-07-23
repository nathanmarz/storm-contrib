package storm.contrib.hbase.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

/*
 * Class that establishes a connection with hbase and returns an HBaseConfiguration object
 */
public class HBaseConnector {
	private HBaseConfiguration conf;
	public final HBaseConfiguration getHBaseConf(final String pathToHBaseXMLFile) {
		conf = new HBaseConfiguration();
		conf.addResource(new Path(pathToHBaseXMLFile));
		return conf;
	}
}
