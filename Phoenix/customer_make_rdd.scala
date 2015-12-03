import scala.collection.JavaConverters._

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.phoenix.spark._

object RDD_Customer {
	def customerToTuple( strArray : Array[String] ) : (Int, String, String, Int, String, Double, String, String) = {
		return (Integer.valueOf(strArray(0)), strArray(1), strArray(2), Integer.valueOf(strArray(3)), strArray(4),strArray(5).toDouble, strArray(6), strArray(7))
	}

	def main(args: Array[String]) {
		val sparkConf = new SparkConf().setAppName("SparkOnPhoenix - part table")
		val sc = new SparkContext(sparkConf)

		val rdd: RDD[Map[String, AnyRef]] = sc.phoenixTableAsRDD(
			"Customer", Seq("C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT"), zkUrl = Some(args(0))
		)

		val cnt = rdd.count()

		val tmpList = List(cnt)

		sc.parallelize(tmpList).saveAsTextFile(args(1))

		sc.stop();
	}
}
