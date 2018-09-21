import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

object Task2 {
  def main(args: Array[String]): Unit = {

    val inputLocation = args(0)
    val outputLocation = args(1)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local[2]")
      .appName("Assignment1")
      .getOrCreate

    val inputFileName = inputLocation + "survey_results_public.csv"

    import spark.implicits._

    val inputDF = spark.read
      .format("csv")
      .option("header", "true")
      .load(inputFileName)
      .select("Country", "Salary", "SalaryType")
      .filter("Salary not in ('NA', '0')").sort("Country")

    val countryRDD = inputDF.select("Country").coalesce(2).rdd.map(r => r(0).toString)
    val interCountryCounts = countryRDD.map(word => (word, 1))


    val startTime = System.currentTimeMillis()
    val countryCounts: RDD[(String, Int)] = interCountryCounts.reduceByKey(_ + _)
    countryCounts.collect()
    val endTime = System.currentTimeMillis()

    val partition1Array:Array[String] = "standard" +:
      countryRDD.mapPartitions(iter => Array(iter.size.toString).iterator, true).collect() :+
      (endTime - startTime).toString

    val countryCountRDD = countryRDD.map(word => (word, 1))
    val customPartitionedCountry:RDD[(String, Int)] = countryCountRDD.partitionBy(new HashPartitioner(2))

    val startTime2 = System.currentTimeMillis()
    val customPartitionCounts = customPartitionedCountry.reduceByKey(_+_)
    customPartitionCounts.collect()
    val endTime2 = System.currentTimeMillis()


    val partition2Array:Array[String] = "partition" +:
      customPartitionedCountry.mapPartitions(iter => Array(iter.size.toString).iterator, true).collect() :+
      (endTime2 - startTime2).toString

    val part2OutputDF = Seq(
      (partition1Array(0), partition1Array(1), partition1Array(2), partition1Array(3)),
      (partition2Array(0), partition2Array(1), partition2Array(2), partition2Array(3))
    ).toDF()

    part2OutputDF.coalesce(1).write
      .format("csv")
      .option("header", "false")
      .mode("overwrite")
      .save(outputLocation)
  }
}
