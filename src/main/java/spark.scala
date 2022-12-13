import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object spark {
  def main(args: Array[String]): Unit = {
    print("Hello Spark")

    val spark = SparkSession
      .builder()
      .appName("kemal")
      .config("spark.master", "local")
      .getOrCreate()

    val players = spark.read.format("csv")
      .option("header", "true")
      .load("/Users/kemaldemirel/Desktop/Lectures/BİL 401/Dataset/players_1920_fin.csv")

    val epl = spark.read
      .format("csv")
      .option("header", "true")
      .load("/Users/kemaldemirel/Desktop/Lectures/BİL 401/Dataset/epl2020.csv")

    def MostAgressivePlayerToParticularTeam( team_name : String) : Dataset[Row] = {
      return players.select("element", "full", "red_cards", "opponent_team")
        .filter(col("opponent_team") === lit(team_name))
        .groupBy("element","full")
        .agg(sum("red_cards"))
        .orderBy(desc("sum(red_cards)"))
    }

    val maxCard = epl
      .select("`Referee.x`","`HR.x`","teamId", "`h_a`")
      .filter("h_a == 'h'")
      .groupBy("`Referee.x`")
      .agg(sum("`HR.x`")
        .as("totalCard"))
      .agg(max("totalCard"))
      .collect()(0)

    val ev_sahibine_en_cok_kirmizi = epl
      .select("`Referee.x`","`HR.x`","teamId","`h_a`")
      .filter("h_a == 'h'")
      .groupBy("`Referee.x`")
      .agg(sum("`HR.x`")
        .as("totalCard"))
      .filter(col("totalCard") === lit(maxCard(0)))



    val angry_referee = epl
      .select("`Referee.x`","`HR.x`","`HY.x`","`AY.x`","`AR.x`")
      .withColumn("totalCard", col("`HR.x`") + col("`HY.x`") + col("`AY.x`") + col("`AR.x`"))
      .groupBy("`Referee.x`")
      .agg(sum("totalCard").as("numOfCards"))
      .orderBy(desc("`numOfCards`"))
      .take(1)

    println("The Angriest Referee" + angry_referee(0))
    ev_sahibine_en_cok_kirmizi.show()
    print(MostAgressivePlayerToParticularTeam("Crystal Palace").show()  )


  }

}
