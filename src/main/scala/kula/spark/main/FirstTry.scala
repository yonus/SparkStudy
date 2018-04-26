package kula.spark.main

import org.apache.spark.{SparkConf, SparkContext}

object FirstTry {
  def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
        val sc = new SparkContext(sparkConf);
        val tf = sc.textFile("/home/pasa/sparkdevelopment/derby.log");
        val splits = tf.flatMap(line => line.split(" ")).map(word =>(word,1))
        val counts = splits.reduceByKey((x,y)=>x+y)
         counts.foreach( x => {
            println(x);
         })
  }
}
