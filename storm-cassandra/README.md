Storm Cassandra Integration
===========================

Integrates Storm and Cassandra by providing a generic and configurable `backtype.storm.Bolt` 
implementation that writes Storm `Tuple` objects to a Cassandra Column Family.

How the Storm `Tuple` data is written to Cassandra is dynamically configurable -- you
provide classes that "determine" a column family, row key, and column name/values, and the 
bolt will write the data to a Cassandra cluster.

### Building from Source

		$ mvn install

### Usage

**Basic Usage**

`CassandraBolt` expects that a Cassandra hostname, port, and keyspace be set in the Storm topology configuration:

		Config config = new Config();
		config.put(CassandraBolt.CASSANDRA_HOST, "localhost");
		config.put(CassandraBolt.CASSANDRA_PORT, 9160);
		config.put(CassandraBolt.CASSANDRA_KEYSPACE, "testKeyspace");
		
		
The `CassandraBolt` class provides a convenience constructor that takes a column family name, and row key field value as arguments:

		IRichBolt cassandraBolt = new CassandraBolt("columnFamily", "rowKey");

The above constructor will create a `CassandraBolt` that writes to the "`columnFamily`" column family, and will look for/use a field 
named "`rowKey`" in the `backtype.storm.tuple.Tuple` objects it receives as the Cassandra row key.

For each field in the `backtype.storm.Tuple` received, the `CassandraBolt` will write a column name/value pair.

For example, given the constructor listed above, a tuple value of:

		{rowKey: 12345, field1: "foo", field2: "bar}

Would yield the following Cassandra row (as seen from `cassandra-cli`):

		RowKey: 12345
		=> (column=field1, value=foo, timestamp=1321938505071001)
		=> (column=field2, value=bar, timestamp=1321938505072000)


#### Dynamic ColumnFamily Determination
[TODO]



#### Dynamic RowKey Generation
[TODO]


### Running the Persistent Word Count Example

The "examples" directory contains a sample `PersistentWordCount` topology that illustrates 
the basic usage of the Cassandra Bolt implementation. It reuses the [`TestWordSpout`](https://github.com/nathanmarz/storm/blob/master/src/jvm/backtype/storm/testing/TestWordSpout.java) 
spout and [`TestWordCounter`](https://github.com/nathanmarz/storm/blob/master/src/jvm/backtype/storm/testing/TestWordCounter.java) 
bolt from the Storm tutorial, and adds an instance of `CassandraBolt` to persist the results.

The `PersistentWordCount` example build the following topology:

	TestWordSpout ==> TestWordCounter ==> CassandraBolt
	
**Data Flow**

1. `TestWordSpout` emits words at random from a pre-defined list.
2. `TestWordCounter` receives a word, updates a counter for that word,
and emits a tuple containing the word and corresponding count ("word", "count").
3. The `CassandraBolt` receives the ("word", "count") tuple and writes it to the
Cassandra database using the word as the row key.

#### Build the Example Source

		$ cd examples
		$ mvn install
	
#### Create Cassandra Schema
Install and run [Apache Cassandra](http://cassandra.apache.org/).

Create the sample schema using `cassandra-cli`:

		$ cd schema
		$ cassandra-cli -h localhost -f cassandra_schema.txt
	
Run the example topology:

		$ mvn exec:java
	
View the end result in `cassandra-cli`:

		$ cassandra-cli -h localhost
		[default@unknown] use stormks;
		[default@stromks] list stormcf;
	
The output should resemble the following:

		Using default limit of 100
		-------------------
		RowKey: nathan
		=> (column=count, value=22, timestamp=1322332601951001)
		=> (column=word, value=nathan, timestamp=1322332601951000)
		-------------------
		RowKey: mike
		=> (column=count, value=11, timestamp=1322332600330001)
		=> (column=word, value=mike, timestamp=1322332600330000)
		-------------------
		RowKey: jackson
		=> (column=count, value=17, timestamp=1322332600633001)
		=> (column=word, value=jackson, timestamp=1322332600633000)
		-------------------
		RowKey: golda
		=> (column=count, value=31, timestamp=1322332602155001)
		=> (column=word, value=golda, timestamp=1322332602155000)
		-------------------
		RowKey: bertels
		=> (column=count, value=16, timestamp=1322332602257000)
		=> (column=word, value=bertels, timestamp=1322332602255000)
		
		5 Rows Returned.
		Elapsed time: 8 msec(s).







