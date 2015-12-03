import scala.collection.JavaConverters._

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._
import org.apache.hadoop.hbase.HBaseConfiguration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

import org.apache.spark._

import java.nio.ByteBuffer

import java.math.BigInteger

object HBasePutModulo {

	def main(args: Array[String]) {
		val sparkConf = new SparkConf().setAppName("HBasePut")
		val sc = new SparkContext(sparkConf)

		// please ensure HBASE_CONF_DIR is on classpath of spark driver
		// e.g: set it through spark.driver.extraClassPath property
		// in spark-defaults.conf or through --driver-class-path
		// command line option of spark-submit

		if (args.length < 1) {
			System.err.println("Usage: HBasePutModulo <table_name> <row_key> <column familiy:column qualifier> <value>")
			System.err.println("Table must exist in HBase")
			System.exit(1)
		}

		val tableName = args(0)

		val conf = HBaseConfiguration.create()

		// Other options for configuring scan behavior are available. More information available at
		// http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/TableInputFormat.html
		conf.set(TableInputFormat.INPUT_TABLE, tableName)

		val myTable = new HTable(conf, args(0))

		val saltKey : Byte = Math.abs((args(1).hashCode % 100)).asInstanceOf[Byte]

		val rowKey = args(1).getBytes()

		val saltedRowKey : Array[scala.Byte] = new Array[scala.Byte](rowKey.length + 1)

		saltedRowKey(0) = saltKey

		for (i <- 0 until rowKey.length) {
			saltedRowKey(i + 1) = rowKey(i)
		}

		val put = new Put( saltedRowKey )
		
		val delimIndex = args(2).indexOf(":")

		if (delimIndex > 0) {
			val columnFamily = args(2).substring(0, delimIndex)
			val columnQualifier = args(2).substring(delimIndex + 1, args(2).length)

			put.addColumn(columnFamily.getBytes(), columnQualifier.getBytes(), args(3).getBytes())	
		} else {
			put.addColumn(args(2).getBytes(), "".getBytes(), args(3).getBytes())
		}

		myTable.put(put)

		myTable.flushCommits()

		sc.stop()

	}
}
