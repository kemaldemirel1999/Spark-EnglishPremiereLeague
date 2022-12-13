import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

import Console._

object spark {
  def main(args: Array[String]): Unit = {
    print("Hello Spark")

    val spark = SparkSession
      .builder()
      .appName("kemal")
      .config("spark.master", "local")
      .getOrCreate()

    val players = spark.read.option("header", true).csv("players_1920_fin.csv")
    val matches = spark.read.option("header", true).csv("epl2020.csv")

    def getMostAggresivePlayerAgainstX = (players: DataFrame, opponent: String) => {
      println("Players with the most cards against ", opponent)
      players.select("element", "full", "yellow_cards", "red_cards", "opponent_team")
        .where(col("opponent_team") === opponent and col("yellow_cards") > 0)
        .groupBy("element", "full").agg(sum("yellow_cards").name("totalYellowCards"), sum("red_cards").name("totalRedCards"))
        .sort(desc("totalYellowCards"))
        .show(false)
    }

    def getMostAggresivePlayer = (players: DataFrame) => {
      println("Players with the most cards")
      players.select("element", "full", "yellow_cards", "red_cards", "opponent_team")
        .where(col("yellow_cards") > 0)
        .groupBy("element", "full").agg(sum("yellow_cards").name("totalYellowCards"), sum("red_cards")
        .name("totalRedCards"))
        .sort(desc("totalYellowCards"))
        .show(false)
    }

    def performanceBetweenTwoWeeks = (players: DataFrame, playerName: String, from: Integer, to: Integer) => {
      val output = players.select("full", "round", "element", "goals_scored", "creativity")
        .where(col("round") >= from
          and col("round") <= to
          and col("goals_scored") > 0)
        .groupBy("full", "element")
        .agg(sum("goals_scored").name("score"),
          avg("creativity").name("creativity"))
        .sort(desc("score"))
        .drop("element")
      output.show()
      output.write.format("csv").save("/Users/omerfarukpolat/Spark-EnglishPremiereLeauge/src/main/output")

    }

    val maxCard = matches
      .select("`Referee.x`","`HR.x`","teamId", "`h_a`")
      .filter("h_a == 'h'")
      .groupBy("`Referee.x`")
      .agg(sum("`HR.x`")
        .as("totalCard"))
      .agg(max("totalCard"))
      .collect()(0)

    val ev_sahibine_en_cok_kirmizi = matches
      .select("`Referee.x`","`HR.x`","teamId","`h_a`")
      .filter("h_a == 'h'")
      .groupBy("`Referee.x`")
      .agg(sum("`HR.x`")
        .as("totalCard"))
      .filter(col("totalCard") === lit(maxCard(0)))



    val angry_referee = matches
      .select("`Referee.x`","`HR.x`","`HY.x`","`AY.x`","`AR.x`")
      .withColumn("totalCard", col("`HR.x`") + col("`HY.x`") + col("`AY.x`") + col("`AR.x`"))
      .groupBy("`Referee.x`")
      .agg(sum("totalCard").as("numOfCards"))
      .orderBy(desc("`numOfCards`"))
      .take(1)

    // println("The Angriest Referee" + angry_referee(0))
    //  ev_sahibine_en_cok_kirmizi.show()
    // print(getMostAggresivePlayerAgainstX(players,"Crystal Palace"))
    // print(getMostAggresivePlayer(players))

    performanceBetweenTwoWeeks(players, "Raheem Sterling", 1, 17)


  }

}
