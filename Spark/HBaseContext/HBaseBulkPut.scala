import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.SparkConf

object HBaseBulkPut {
    def main(args: Array[String]) {
        if (args.length < 2) {
            println("HBaseBulkPut {HDFS Filepath} {Table Name} {Column Family} {The Number of Column Qualifier} {Column Qualifier1} [Column Qualifiers...]" )
            return
        }

        val filePath = args(0)
        val tableName = args(1)
        val columnFamily = Bytes.toBytes(args(2))
        val numberOfQualifier = args(3).toInt
        
        val qualifiers = new Array[Array[Byte]](numberOfQualifier)

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
                    val put = new Put(Bytes.toBytes(putRecord(0)))
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
