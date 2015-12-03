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

object HBaseGet {
  type HBaseRow = java.util.NavigableMap[Array[Byte], java.util.NavigableMap[Array[Byte], java.util.NavigableMap[java.lang.Long, Array[Byte]]]]
  type CFTimeseriesRow = Map[Array[Byte], Map[Array[Byte], Map[Long, Array[Byte]]]]
  type CFTimeseriesRowStr = scala.collection.immutable.Map[String, scala.collection.immutable.Map[String, scala.collection.immutable.Map[Long, String]]]

  def navMapToMap(navMap: HBaseRow): CFTimeseriesRow =
    navMap.asScala.toMap.map(cf =>
      (
        cf._1,
        cf._2.asScala.toMap.map(col =>
          (col._1, col._2.asScala.toMap.map(elem => (elem._1.toLong, elem._2)))
        )
      )
    )

  def rowToStrMap(navMap: CFTimeseriesRow): CFTimeseriesRowStr =
    navMap.map(cf =>
      (
         Bytes.toString(cf._1),
         cf._2.map(col =>
          (Bytes.toString(col._1), col._2.map(elem => (elem._1, Bytes.toString(elem._2))))
        )
      )
    )

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("HBaseGet")
    val sc = new SparkContext(sparkConf)

    // please ensure HBASE_CONF_DIR is on classpath of spark driver
    // e.g: set it through spark.driver.extraClassPath property
    // in spark-defaults.conf or through --driver-class-path
    // command line option of spark-submit

    val tableName = args(0)

    val conf = HBaseConfiguration.create()

    // Other options for configuring scan behavior are available. More information available at
    // http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/TableInputFormat.html
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    var mapredHBase = hBaseRDD.map(kv => 
      (kv._1.get(), navMapToMap(kv._2.getMap))
    ).map(kv =>
      (kv._1, rowToStrMap(kv._2))
    ).map(kv => {
      /*
      kv._1 -> Array[Byte] row key
      kv._2 -> map( String key, map ( String, map ( Long, String ) ) )
      kv._2 -> map( column family name, map ( column qualifier name, map ( timestamp, value ) ) )
      */
      (Bytes.toString(kv._1), kv._2)
    })
    .saveAsHadoopFile(args(1), classOf[String], classOf[CFTimeseriesRowStr], classOf[TextOutputFormat[Text,Text]])

    sc.stop()
  }
}
