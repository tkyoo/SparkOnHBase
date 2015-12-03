import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.SparkConf

import java.math.BigInteger

object HBaseBulkPutModulo {
    def main(args: Array[String]) {
        if (args.length < 2) {
            println("HBaseBulkPutModulo {HDFS Filepath} {Table Name} {Column Family} {The Number of Column Qualifier} {Column Qualifier1} [Column Qualifiers...]" )
            return
        }

        val filePath = args(0)
        val tableName = args(1)
        val columnFamily = Bytes.toBytes(args(2))
        val numberOfQualifier = args(3).toInt
        
        val qualifiers = new Array[Array[scala.Byte]](numberOfQualifier)

        for ( i <- 0 until numberOfQualifier) {
            qualifiers(i) = Bytes.toBytes(args(i + 4));
        }

        val sparkConf = new SparkConf().setAppName("HBaseBulkPut: " + tableName + " " + columnFamily)
        val sc = new SparkContext(sparkConf)

        try {
            val rdd = sc.textFile(filePath).map( line => {
                   line.split("\\|")
               }
            )

            val hbaseConf = HBaseConfiguration.create()
            val hbaseContext = new HBaseContext(sc, hbaseConf)

            hbaseContext.bulkPut[Array[String]](rdd,
                TableName.valueOf(tableName),
                (putRecord) => {
					val saltKey : Byte = Math.abs((putRecord(0).hashCode % 100)).asInstanceOf[Byte]
					val rowKey = putRecord(0).getBytes

					val saltedRowKey : Array[Byte] = new Array[Byte](1 + rowKey.length)

					saltedRowKey(0) = saltKey

					for ( i <- 0 until rowKey.length ) {
						saltedRowKey(i + 1) = rowKey(i)
					}

                    val put = new Put( saltedRowKey )

                    for ( i <- 0 until numberOfQualifier) {
                        put.addColumn(columnFamily, qualifiers(i), Bytes.toBytes(putRecord(i)))
                    }

                    (put)
                });
        } finally {
            sc.stop()
        }
    }
}
