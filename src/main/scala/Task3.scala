import org.apache.spark.sql.functions._

object Task3 {
  def main(args: Array[String]): Unit = {

    val inputLocation = args(0)
    val outputLocation = args(1)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local[2]")
      .appName("Assignment1")
      .getOrCreate

    val inputFileName = inputLocation + "survey_results_public.csv"

    val inputDF = spark.read
      .format("csv")
      .option("header", "true")
      .load(inputFileName)
      .select("Country", "Salary", "SalaryType")
      .filter("Salary not in ('NA', '0')").sort("Country")

    val inputDF3 = inputDF.withColumn("Country", inputDF.col("Country"))
      .withColumn(
        "Salary",
        when(inputDF.col("SalaryType") === lit("Monthly"), regexp_replace(inputDF.col("Salary").cast("String"), lit(","), lit("")).cast("Double") * 12)
          .when(inputDF.col("SalaryType") === lit("Weekly"), regexp_replace(inputDF.col("Salary").cast("String"), lit(","), lit("")).cast("Double") * 52)
          .otherwise(regexp_replace(inputDF.col("Salary").cast("String"), lit(","), lit("")).cast("Double")*  1)
      ).withColumn("SalaryType", inputDF.col("SalaryType") )

    val finalDF = inputDF3.groupBy("Country").agg(count("Salary"),
      min("Salary").cast("Integer"),  max("Salary").cast("Integer"), bround(avg("Salary").cast("Double"),2))

    finalDF.coalesce(1)
      .write.mode("overwrite")
      .format("csv")
      .option("header", "false")
      .csv(outputLocation)
  }
}
