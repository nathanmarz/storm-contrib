package storm.contrib.hbase.utils;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * This class implements methods for checking an hbase table's existence, creating an hbase table and inserting a row into the hbase table
 */
public class HBaseCommunicator {

	private String colFamilyName = null, colValue = null, result = null;
	private byte[] rowKeyBytes = null, key = null, columnValue = null;

	private static HBaseConfiguration conf;
	private HTable table = null;
	private Put putdata = null;
	private HBaseAdmin admin = null;
	private Get getRowObj = null; 
	private Result rowEntries = null;

	public HBaseCommunicator(final HBaseConfiguration conf)	{
		this.conf = conf;
	}

	/*
	 * check if the table exists
	 */
	public final boolean tableExists(final String tableName) {
		try {
			admin = new HBaseAdmin(conf);
			if (admin.tableExists(tableName)) {
				return true;
			}
		} catch (Exception e) {
			System.out.println("Exception occured while checking table's existence");
			e.printStackTrace();
		}
		return false;
	}

	/*
	 * creates a table 
	 */
	public final void createTable(final String tableName, final ArrayList<String> colFamilies) {
		try {
			HBaseAdmin hbase = new HBaseAdmin(conf);
			HTableDescriptor desc = new HTableDescriptor(tableName);
			for(int i = 0; i < colFamilies.size(); i++) {
				HColumnDescriptor meta = new HColumnDescriptor(colFamilies.get(i).getBytes());
				desc.addFamily(meta);
			}
			hbase.createTable(desc);
		} catch (Exception e) {
			System.out.println("Exception occured creating table in hbase");
			e.printStackTrace();
		}
	}

	/*
	 * add row to a table
	 */
	public final void addRow(final String rowKey, final String tableName, final ArrayList<String> colFamilies, final ArrayList<ArrayList<String>> colNames, final ArrayList<ArrayList<String>> data) {
		try	{
			colFamilyName = null;
			rowKeyBytes = null;
			putdata = null;
			table = new HTable(conf, tableName);
			//rowKey = "row" + (int)(Math.random() * 1000);
			rowKeyBytes = Bytes.toBytes(rowKey);
			putdata = new Put(rowKeyBytes);
			for (int i = 0 ; i < colFamilies.size(); i++) {
				colFamilyName = colFamilies.get(i);
				if (colNames.get(i).size() == data.get(i).size())
				{
					for (int j = 0 ; j < colNames.get(i).size(); j++) {
						colValue = data.get(i).get(j);
						if (colValue.equals(null))
							colValue = "null";
						putdata.add(Bytes.toBytes(colFamilyName), Bytes.toBytes(colNames.get(i).get(j)),
								Bytes.toBytes(colValue));
					}
					table.put(putdata);
				}
			}
		} catch (IOException e)	{
			System.out.println("Exception occured in adding data");
		}
	}
	
	/*
	 * obtain value of the column of an hbase table
	 */
	public final String getColEntry(final String tableName, final String rowKey,
			final String colFamilyName, final String colName) {
		result = null;
		try {
			HTable table = new HTable(conf, tableName);
			key = Bytes.toBytes(rowKey);
			getRowObj = new Get(key);
			rowEntries = table.get(getRowObj);
			columnValue = rowEntries.getValue(Bytes.toBytes(colFamilyName),
					Bytes.toBytes(colName));
			result = Bytes.toString(columnValue);
		} catch (IOException e) {
			System.out.println("Exception occured in retrieving data");
		}
		return result;
	}

}
