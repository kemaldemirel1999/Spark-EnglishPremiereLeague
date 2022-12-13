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

    def getShotOnTargetRatios = (matches: DataFrame) => {
      val awayTeamsShots = matches.select("teamId", "`HS.x`", "`HST.x`", "h_a").where(col("h_a") === "a").groupBy(col("teamId")).agg(sum(col("`HS.x`")).name("TotalShotsWhenAway"), sum(col("`HST.x`")).name("TotalShotsOnTargetWhenAway"))
      // awayTeamsShots.withColumn("RatioShotsOnTargetPerMatchWhenAway", col("TotalShotsOnTargetWhenAway") / col("TotalShotsWhenAway")).show()
      val homeTeamsShots = matches.select("teamId", "`HS.x`", "`HST.x`", "h_a").where(col("h_a") === "h").groupBy(col("teamId")).agg(sum(col("`HS.x`")).name("TotalShotsWhenHome"), sum(col("`HST.x`")).name("TotalShotsOnTargetWhenHome"))
      val homeTeamsShotRatios = homeTeamsShots.withColumn("RatioShotsOnTargetPerMatchWhenHome", round(col("TotalShotsOnTargetWhenHome") / col("TotalShotsWhenHome"), 3))
      val awayTeamsShotRatios = awayTeamsShots.withColumn("RatioShotsOnTargetPerMatchWhenAway", round(col("TotalShotsOnTargetWhenAway") / col("TotalShotsWhenAway"), 3))

      val ratioTable = awayTeamsShotRatios.join(homeTeamsShotRatios, awayTeamsShotRatios("teamId") === homeTeamsShotRatios("teamId"))
        .drop(homeTeamsShotRatios("teamId"))
        .select("teamId", "TotalShotsWhenHome", "TotalShotsOnTargetWhenHome", "RatioShotsOnTargetPerMatchWhenHome",
          "RatioShotsOnTargetPerMatchWhenAway", "TotalShotsWhenAway", "TotalShotsOnTargetWhenAway")
        .sort(desc("RatioShotsOnTargetPerMatchWhenHome"))

      ratioTable.write.option("header", true).format("csv").save("/Users/omerfarukpolat/Spark-EnglishPremiereLeauge/src/main/ratioTable")

    }

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
      val path = System.getProperty("user.dir") + "/src/main/output"
      output.write.format("csv").save(path)

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
    //getShotOnTargetRatios(matches)

  }

}
