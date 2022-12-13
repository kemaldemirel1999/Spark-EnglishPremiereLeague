import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object spark {
  def main(args: Array[String]): Unit = {
    print("Hello Spark")

    val spark = SparkSession
      .builder()
      .appName("kemal")
      .config("spark.master", "local")
      .getOrCreate()

    val players = spark.read.format("csv").option("header", "true").load("/Users/kemaldemirel/Desktop/Lectures/BİL 401/Dataset/players_1920_fin.csv")

    //players.select("element", "full", "red_cards", "opponent_team")
    // .filter("opponent_team == 'Crystal Palace'")
    // .groupBy("element","full").count()
    //.agg(sum("red_cards"))
      //.orderBy(desc("sum(minutes)"))
     // .show()

    val epl = spark.read
      .format("csv")
      .option("header", "true")
      .load("/Users/kemaldemirel/Desktop/Lectures/BİL 401/Dataset/epl2020.csv")

    val maxCard = epl
      .select("`Referee.x`","`HR.x`","teamId", "`h_a`")
      .filter("h_a == 'h'")
      .groupBy("`Referee.x`")
      .agg(sum("`HR.x`")
        .as("totalCard"))
      .agg(max("totalCard"))
      .collect()(0)

    epl
      .select("`Referee.x`","`HR.x`","teamId","`h_a`")
      .filter("h_a == 'h'")
      .groupBy("`Referee.x`")
      .agg(sum("`HR.x`")
        .as("totalCard"))
      .filter(col("totalCard") === lit(maxCard(0)))
      .show()


    val angry_referee = epl
      .select("`Referee.x`","`HR.x`","`HY.x`","`AY.x`","`AR.x`")
      .withColumn("totalCard", col("`HR.x`") + col("`HY.x`") + col("`AY.x`") + col("`AR.x`"))
      .groupBy("`Referee.x`")
      .agg(sum("totalCard").as("numOfCards"))
      .orderBy(desc("`numOfCards`"))
      .take(1)

    println("The Angriest Referee" + angry_referee(0))




  }

}
