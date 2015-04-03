import grizzled.slf4j.Logger

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes

case class MyRating(userId: Int, link: MyLink, time: Long)
case class MyLink(url: String, title: String) extends Ordered[MyLink] {
  def compare(another: MyLink): Int = this.url compare another.url
}

object LinkRec {
  val logger = Logger("LinkRec")

  def main(args: Array[String]) {
    if (args.length != 1) {
      println("Usage: YOUR_SPARK_HOME/bin/spark-submit --class LinkRec --master yarn-cluster target/scala-*/*.jar <userId>")
      sys.exit(1)
    }

    // set up environment
    val conf: SparkConf = new SparkConf()
                            .setAppName("LinkRec")
                            .setMaster("local[*]")
                            .set("spark.ui.enabled", "false")

    val sc: SparkContext = new SparkContext(conf)

    // load data, data in MyRating
    logger.warn("start loading data")
    val trainingData: RDD[MyRating] = loadDataFromDB(sc).cache()
    logger.warn("complete loading data")
    // println("[XC] Data: ")
    // data.collect().foreach(println)

    // create mapping
    val allLinks = trainingData.map(_.link).distinct()

    val linkToInt: RDD[(MyLink, Long)] = allLinks.zipWithUniqueId()
    val intToLink: RDD[(Long, MyLink)] = linkToInt map { case (l, r) => (r, l) }

    // FIXME: I don't know how to annnotate Map here..
    val linkMap = linkToInt.collectAsMap map { case (l, r) => (l, r.toInt) }
    val reversedLinkMap = intToLink.collectAsMap

    val trainingRating: RDD[Rating] = trainingData.map { r =>
      Rating(r.userId, linkMap(r.link), 1.0)
    }

    // println("[XC] trainingData: ")
    // trainingData.collect().foreach(println)

    // Build the recommendation model using ALS
    logger.warn("start training data")
    var model: MatrixFactorizationModel = getBestModel(trainingRating)
    logger.warn("complete training data")

    // get target user id
    val userId = args(0).toInt

    val sharedLinks = trainingData.filter(_.userId == userId).map(_.link)
    val unsharedLinks = allLinks.subtract(sharedLinks)

    val userData = unsharedLinks.map(l => (userId, linkMap(l)))

    // println("[XC] User Data: ")
    // userData.collect().foreach(println)

    // recommendation
    logger.warn("start prediction")
    val predictions = model.predict(userData).filter(_.rating >= 0).sortBy(-_.rating).take(50)
    logger.warn(predictions.mkString("\n"))

    var reclinks = predictions.map(p => reversedLinkMap(p.product))

    // ranking TODO
    logger.warn("start building map")

    var reclinksWithTitle = reclinks.map(link =>
                            "{\"url\":\"" + link.url + "\", \"title\":\"" + link.title + "\"}");

    print("{\"reclinks\": [" + reclinksWithTitle.mkString(", ") + "]}")

    // val reclinks = model.recommendProducts(userID, 50)
    // println(reclinks.mkString("\n"))

    // clean up
    sc.stop()
  }

  def loadDataFromDB(sc: SparkContext): RDD[MyRating] = {
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "linkrec")

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val ratings = hBaseRDD.map(item => item._2.raw())
                    .flatMap(_.map (cell =>
                      MyRating(
                        Bytes.toString(CellUtil.cloneRow(cell)).toInt,
                        MyLink(
                            Bytes.toString(CellUtil.cloneQualifier(cell)),
                            Bytes.toString(CellUtil.cloneValue(cell))),
                        cell.getTimestamp())))

    return ratings
  }

  def getBestModel(data: RDD[Rating]): MatrixFactorizationModel = {
    val rank = 5
    val numIterations = 10
    val model = ALS.trainImplicit(data, rank, numIterations)
    return model;
  }
}
