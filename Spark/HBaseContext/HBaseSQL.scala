import scala.collection.JavaConverters._

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._
import org.apache.hadoop.hbase.client.{HBaseAdmin,Scan,Result}
import org.apache.hadoop.hbase.mapreduce.{TableMapper,TableMapReduceUtil}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat
import org.apache.hadoop.io.{Text,NullWritable}

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

import org.apache.spark._

import org.apache.hadoop.io.NullWritable

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

import java.nio.ByteBuffer

// SPARK SQL
import org.apache.spark.sql.SQLContext

import org.apache.hadoop.hbase.spark.HBaseContext

object HBaseSQL {
    def main(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("HBase SparkOnSQL")
        val sc = new SparkContext(sparkConf)

        val sqlContext = new SQLContext(sc)

        val conf = HBaseConfiguration.create()

        val hbaseContext = new HBaseContext(sc, conf)

        // Examples: SQL Group by
        // http://www.w3schools.com/sql/sql_groupby.asp

        val orders = sqlContext.read.format("org.apache.hadoop.hbase.spark")
        .options(
            Map("hbase.columns.mapping" ->
                "OrderID STRING :key, CustomerID STRING 0:CustomerID, EmployeeID STRING 0:EmployeeID, OrderDate STRING 0:OrderDate, ShipperID STRING 0:ShipperID",
                "hbase.table" -> "orders")
        ).load()

        orders.registerTempTable("orders")

        val shippers = sqlContext.read.format("org.apache.hadoop.hbase.spark")
        .options(
            Map("hbase.columns.mapping" ->
                "ShipperID STRING :key, ShipperName STRING 0:ShipperName, Phone STRING 0:Phone",
                "hbase.table" -> "Shippers")
        ).load()

        shippers.registerTempTable("Shippers")

        // The results of SQL queries are DataFrame and support all the normal RDD operations.
        // The columns of a row in the result can be accessed by field index
		val results = sqlContext.sql("select Shippers.ShipperName, COUNT(orders.OrderID) as NumberOfOrders from orders left join Shippers on orders.ShipperID = Shippers.ShipperID group by ShipperName")

        // args(0): HDFS filepath
        results.write.format("json").save(args(0))

        sc.stop()
    }
}
