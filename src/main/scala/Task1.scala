import org.apache.spark.rdd.RDD

object Task1 {
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

    val totalRecords = inputDF.count()
    val countryRDD = inputDF.select("Country").coalesce(2).rdd.map(r => r(0).toString)
    val interCountryCounts = countryRDD.map(word => (word, 1))
    val countryCounts: RDD[(String, Int)] = interCountryCounts.reduceByKey(_ + _)

    val countryDF = countryCounts.sortBy(_._1.toString)
      .toDF("Total", totalRecords.toString)

    countryDF.coalesce(1).write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save(outputLocation)
  }
}
