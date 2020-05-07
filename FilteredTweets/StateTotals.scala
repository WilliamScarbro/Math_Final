import org.apache.hadoop.fs.{FileSystem,Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object StateTotals {
    def main(args: Array[String]) {
        val sc = SparkContext.getOrCreate()

        val files = FileSystem.get(sc.hadoopConfiguration).listStatus(new Path("hdfs://juneau:11111/math435/filtered"))
        files.foreach{ file =>
            val filename = file.getPath.toString
            processFile(filename)
        }
    }

    def processFile(filename: String) {
        val spark = SparkSession.builder.appName("StateTotals").getOrCreate()
        import spark.implicits._

        val textFile = spark.read.format("csv").load(filename)
            .filter("_c0 is not null")
            .filter("_c1 is not null")
            .filter("_c2 is not null")

        val data = textFile.rdd.map{ row =>
            // (date, 1)
            (row.getString(1), 1.toLong)
        }.reduceByKey{ (x,y) =>
            x + y
        }.sortBy(_._1)

        val outFile = filename.replace("filtered","results").replace(".csv","")
        data.toDF.write.format("csv").save(outFile)
    }
}
