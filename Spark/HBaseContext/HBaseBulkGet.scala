import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.{CellUtil, TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.SparkConf

object HBaseBulkGet {
    def main(args: Array[String]) {
        if (args.length < 2) {
            println("HBaseBulkGet {Table Name} {RowKey} {Max Versions}" )
            return
        }

        val tableName = args(0)
        val rowKey = Bytes.toBytes(args(1))

        val sparkConf = new SparkConf().setAppName("HBaseGet: " + tableName + " " + rowKey)
        val sc = new SparkContext(sparkConf)

        try {
            val rdd = sc.parallelize(Array(rowKey))

            val hbaseConf = HBaseConfiguration.create()
            val hbaseContext = new HBaseContext(sc, hbaseConf)

            val getRdd = hbaseContext.bulkGet[Array[Byte], String](
                TableName.valueOf(tableName),
                1,
                rdd,
                record => {
                    val tmpGet = new Get(record)
                    tmpGet.setMaxVersions(Integer.valueOf(args(2)))
                },
                (result: Result) => {
                    val it = result.listCells().iterator()
                    val b = new StringBuilder

                    b.append(Bytes.toString(result.getRow) + ":")

                    while (it.hasNext) {
                        val cell = it.next()
                        val q = Bytes.toString(CellUtil.cloneQualifier(cell))
                        if (q.equals("counter")) {
                            b.append("(" + q + "," + cell.getTimestamp() + "," + Bytes.toLong(CellUtil.cloneValue(cell)) + ")")
                        } else {
                            b.append("(" + q + "," + cell.getTimestamp() + "," + Bytes.toString(CellUtil.cloneValue(cell)) + ")")
                        }
                    }

                    b.toString()
                }
            )

            getRdd.collect().foreach(v => println(v))
        } finally {
            sc.stop()
        }
    }
}
