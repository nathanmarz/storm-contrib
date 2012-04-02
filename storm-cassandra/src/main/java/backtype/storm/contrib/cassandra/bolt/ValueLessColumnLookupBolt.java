// Copyright (c) 2012 P. Taylor Goetz

package backtype.storm.contrib.cassandra.bolt;

import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * A bolt implementation that emits tuples based on a combination of cassandra
 * rowkey, collumnkey, and delimiter.
 * <p/>
 * When this bolt received a tuple, it will attempt the following:
 * <ol>
 * <li>Look up a value in the tuple using <code>rowKeyField</code></li>
 * <li>Fetch the corresponding row from cassandra</li>
 * <li>Fetch the column <code>columnKeyField</code> value from the row.</li>
 * <li>Split the column value into an array based on <code>delimiter</code></li>
 * <li>For each value, emit a tuple with <code>{emitIdFieldName}={value}</code></li>
 * </ol>
 * For example, given the following cassandra row: <br/>
 * 
 * <pre>
 * RowKey: mike
 * => (column=followers, value=john:bob, timestamp=1328848653055000)
 * </pre>
 * 
 * and the following bolt setup:
 * 
 * <pre>
 * rowKeyField = "rowKey"
 * columnKeyField = "followers"
 * delimiter = ":"
 * emitIdFieldName = "rowKey"
 * emitValueFieldName = "follower"
 * </pre>
 * 
 * if the following tuple were received by the bolt:
 * 
 * <pre>
 * {rowKey:mike}
 * </pre>
 * 
 * The following tuples would be emitted:
 * 
 * <pre>
 * {rowKey:mike, follower:john}
 * {rowKey:mike, follower:bob}
 * </pre>
 * 
 * @author tgoetz
 */
@SuppressWarnings("serial")
public class ValueLessColumnLookupBolt extends BaseCassandraBolt {

    private static final Logger LOG = LoggerFactory
                    .getLogger(ValueLessColumnLookupBolt.class);
    private String columnFamily;
    private String rowKeyField;

    private String emitIdFieldName;
    private String emitValueFieldName;

    private boolean isDrpc = false;

    public ValueLessColumnLookupBolt(String columnFamily, String rowKeyField,
                    String emitIdFieldName, String emitValueFieldName,
                    boolean isDrpc) {
        super();
        this.columnFamily = columnFamily;
        this.rowKeyField = rowKeyField;
        this.emitIdFieldName = emitIdFieldName;
        this.emitValueFieldName = emitValueFieldName;
        this.isDrpc = isDrpc;
    }

    public ValueLessColumnLookupBolt(String columnFamily, String rowKeyField,
                    String columnKeyField, String delimiter,
                    String emitIdFieldName, String emitValueFieldName) {
        this(columnFamily, rowKeyField, 
                        emitIdFieldName, emitValueFieldName, false);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String rowKey = input.getStringByField(this.rowKeyField);

        ColumnFamilyTemplate<String, String> template = new ThriftColumnFamilyTemplate<String, String>(
                        this.keyspace, this.columnFamily,
                        new StringSerializer(), new StringSerializer());

        ColumnFamilyResult<String, String> result = template
                        .queryColumns(rowKey);
        for(String val : result.getColumnNames()){
            if (this.isDrpc) {
                collector.emit(new Values(input.getValue(0), rowKey, val));
            }
            else {
                collector.emit(new Values(rowKey, val));
            }
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (this.isDrpc) {
            declarer.declare(new Fields("id", this.emitIdFieldName,
                            this.emitValueFieldName));
        }
        else {
            declarer.declare(new Fields(this.emitIdFieldName,
                            this.emitValueFieldName));
        }

    }

}
