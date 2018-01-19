package demo

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

/**
  * //@Author: fansy
  * //@Time: 2018/1/18 15:50
  * //@Email: fansy1990@foxmail.com
  */
object Es2Hive3 {
  def main(args: Array[String]): Unit = {
    if(args.length!= 6){
      println("args.length:"+args.length)
      System.exit(-1)
    }
    val (esNodeIp,esNodePort,esTable,hiveTable,columns,partitions) = (args(0),args(1),args(2),args(3),args(4),args(5).toInt)
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    // Use HiveContext not SQLContext
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val options = Map(
      ("es.nodes", esNodeIp),
      ("es.port", esNodePort.toString),
      ("es.read.metadata", "false"),
      ("es.mapping.date.rich", "false"),
      ("es.batch.size.bytes","10mb"),
      ("es.batch.size.entries","10000")
    )
    val default_query: String = "?q=*:*"
    val result = sqlContext.esDF(esTable, default_query, options)
    val columnsArr = columns.split(",").map(_.trim)
    result.select(columnsArr.head, columnsArr.tail: _*)
      .repartition(partitions).write.saveAsTable(hiveTable)
    sc.stop()
  }
}
