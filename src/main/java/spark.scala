import org.apache.spark.sql.SparkSession

object spark {
  def main(args: Array[String]): Unit = {
    print("zd")

    val spark = SparkSession
      .builder()
      .appName("kemal")
      .config("spark.master", "local")
      .getOrCreate()

    val wtf = spark
      .read
      .text("/Users/kemaldemirel/IdeaProjects/kemal2/pom.xml")

    wtf.show()

  }
}
