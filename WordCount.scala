package PracticeSpark

/**
  * Created by xdvi2 on 11/09/2017.
  */
import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config._
import org.apache.hadoop.fs._

object WordCount {
  def main(args:Array[String]): Unit ={

    val props: Config = ConfigFactory.load()
    val env = args(2)
    val conf = new SparkConf().setAppName("WordCount")
                              .setMaster(props.getConfig(env).getString("executionMode"))
                              .set("spark.ui.port","12345")
    val sc = new SparkContext(conf)
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val inputpath = args(0)
    val outputpath = args(1)

    val inpath = new Path(inputpath)
    val outpath = new Path(outputpath)

    if(fs.exists(inpath)) {
      if(fs.exists(outpath)) {
        fs.delete(outpath, true)
      }
      else {
      sc.textFile(inputpath).flatMap(rec => rec.split(' ')).
        map(x => (x, 1)).reduceByKey((x, y) => x + y).
        map(rec => rec._1 + "\t" + rec._2).saveAsTextFile(outputpath)
    }
    }
    else
      println("Input path does not exists")
  }

}
