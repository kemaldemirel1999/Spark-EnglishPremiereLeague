import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

import Console._


object spark {

  import sys.process._

  def callPython(): Unit = {
    val result = "python3 bigDataPlots.py" ! ProcessLogger(stdout append _, stderr append _)
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("kemal")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val players = spark.read.option("header", true).csv("players_1920_fin.csv")
    val matches = spark.read.option("header", true).csv("epl2020.csv")

    def getFoulsOfTeams = (matches: DataFrame) => {
      matches.groupBy("teamId").agg(sum("`HF.x`").name("Home Fouls"), sum("`AF.x`").name("Away Fouls")).show()
    }

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

      val path = System.getProperty("user.dir") + "/src/main/ratioTable"
      ratioTable.write.option("header", true).format("csv").save(path)

    }

    def getMostAggresivePlayerAgainstX(players: DataFrame, opponent: String): Dataset[Row] = {
      return players.select("element", "full", "yellow_cards", "red_cards", "opponent_team")
        .where(col("opponent_team") === opponent and col("yellow_cards") > 0)
        .groupBy("element", "full").agg(sum("yellow_cards").name("totalYellowCards"), sum("red_cards").name("totalRedCards"))
        .sort(desc("totalYellowCards"))
    }

    def getMostAggresivePlayer(players: DataFrame): Dataset[Row] = {
      //println("Players with the most cards")
      return players.select("element", "full", "yellow_cards", "red_cards", "opponent_team")
        .where(col("yellow_cards") > 0)
        .withColumn("totalCard", col("yellow_cards") + col("red_cards"))
        .groupBy("element", "full").agg(sum("yellow_cards").name("totalYellowCards"), sum("red_cards").name("totalRedCards"),
        sum("totalCard").as("numOfCards"))
        .sort(desc("numOfCards"))
    }

    def getTeamSquads = (matches: DataFrame, players: DataFrame, team: String) => {
      val playersWithStatistics = players.select("full", "team", "goals_scored", "assists")
        .groupBy("full", "team")
        .agg(sum(col("goals_scored")).name("Total Goals"),
          sum(col("assists")).name("Total Assists"))
      matches.join(playersWithStatistics, matches("teamId") === players("team"))
        .filter(col("teamId") === team)
        .select("teamId", "full", "Total Goals", "Total Assists")
        .distinct()
        .sort(desc("Total Goals")).show(100)
    }

    def getFixtureOfATeam = (matches: DataFrame, team: String) => {
      val matchesDatesReferees = matches
      .select("teamId", "`Referee.x`", "date", "h_a")
      .withColumn("Referee", col("`Referee.x`"))
      .withColumn("date2", col("date"))
      .withColumn("opponent", col("teamId"))
      .withColumn("homeAway", col("h_a"))
      .drop("Referee.x", "date", "teamId", "h_a")

      matches
      .join(matchesDatesReferees, matches("date") === matchesDatesReferees("date2")
      && matches("`Referee.x`") === matchesDatesReferees("Referee") && matches("h_a") === "h"
      && matchesDatesReferees("homeAway") === "a")
      .select("teamId", "opponent", "scored", "missed")
      .where(col("opponent") === team or col("teamId") === team).show()
    }

    def getMatchesAwayTeamBelowxG = (matches: DataFrame, team: String) => {
      val matchesDatesReferees = matches.select("teamId", "`Referee.x`", "date", "h_a")
      .withColumn("Referee", col("`Referee.x`"))
      .withColumn("date2", col("date"))
      .withColumn("opponent", col("teamId"))
      .withColumn("homeAway", col("h_a"))
      .drop("Referee.x", "date", "teamId", "h_a")

      matches.join(matchesDatesReferees, matches("date") === matchesDatesReferees("date2")
        && matches("`Referee.x`") === matchesDatesReferees("Referee")
        && matches("h_a") === "h"
        && matchesDatesReferees("homeAway") === "a")
        .select("teamId", "opponent", "scored", "missed", "xG", "xGA")
        .where(col("opponent") === team
          and col("xGA") > col("missed"))
        .withColumn("opponent_scored", col("missed"))
        .drop("missed")
        .select("teamId", "opponent", "scored", "opponent_scored", "xGA").show()
    }

    def performanceBetweenTwoWeeks = (players: DataFrame, from: Integer, to: Integer) => {
      val output = players.select("full", "round", "element", "goals_scored", "creativity")
        .where(col("round") >= from
          and col("round") <= to
          and col("goals_scored") > 0)
        .groupBy("full", "element")
        .agg(sum("goals_scored").name("score"),
          avg("creativity").name("creativity"))
        .sort(desc("score")).withColumn("creativity", round(col("creativity"), 2))
        .drop("element")
      output.show()


      val path = System.getProperty("user.dir") + "/src/main/performanceTable"
      output.write.option("header", true).format("csv").save(path)

    }

    def joinQuery = (matches: DataFrame, players: DataFrame, team: String, player: String) => {
      val matchesDatesReferees = matches
      .select("teamId", "`Referee.x`", "date", "h_a")
      .withColumn("Referee", col("`Referee.x`"))
      .withColumn("date2", col("date"))
      .withColumn("opponent", col("teamId"))
      .withColumn("homeAway", col("h_a"))
      .drop("Referee.x", "date", "teamId", "h_a")

      val matchesExpanded = matches
      .join(matchesDatesReferees,
      matches("date") === matchesDatesReferees("date2")
      && matches("`Referee.x`") === matchesDatesReferees("Referee")
       && matches("h_a") === "h" && matchesDatesReferees("homeAway") === "a")
       .withColumn("opponent_scored", col("missed"))
       .drop("missed")

      matchesExpanded.join(players, matchesExpanded("date") === players("kickoff_time")
       && matchesExpanded("teamId") === players("team")
       && matchesExpanded("opponent") === players("opponent_team"))
       .select("full", "team", "opponent_team",
       "`HS.x`", "`HST.x`", "`HF.x`", "`HC.x`","`HY.x`", "assists", "creativity", "goals_scored")
       .where(col("team") === team
       and col("full") === player
        and col("minutes") > 0).show()
    }

    val maxCard = matches
      .select("`Referee.x`", "`HR.x`", "teamId", "`h_a`")
      .filter("h_a == 'h'")
      .groupBy("`Referee.x`")
      .agg(sum("`HR.x`")
        .as("totalCard"))
      .agg(max("totalCard"))
      .collect()(0)

    val mostCardsToHomeTeam = matches
      .select("`Referee.x`", "`HR.x`", "teamId", "`h_a`")
      .filter("h_a == 'h'")
      .groupBy("`Referee.x`")
      .agg(sum("`HR.x`")
        .as("totalCard"))
      .filter(col("totalCard") === lit(maxCard(0)))


    val angry_referee = matches
      .select("`Referee.x`", "`HR.x`", "`HY.x`", "`AY.x`", "`AR.x`").withColumn("Referees", col("`Referee.x`"))
      .withColumn("totalCard", col("`HR.x`") + col("`HY.x`") + col("`AY.x`") + col("`AR.x`"))
      .groupBy("`Referee.x`")
      .agg(sum("totalCard").as("numOfCards"))
      .orderBy(desc("`numOfCards`"))

    angry_referee.withColumnRenamed("Referee.x", "Referees")


    val refereeTablePath = System.getProperty("user.dir") + "/src/main/refereeTable"
    angry_referee.write.option("header", true).format("csv").save(refereeTablePath)

    angry_referee.show()
    mostCardsToHomeTeam.show()
    getMostAggresivePlayerAgainstX(players, "Crystal Palace").show()
    getMostAggresivePlayer(players).show()

    performanceBetweenTwoWeeks(players, 1, 17)
    getShotOnTargetRatios(matches)

    getFoulsOfTeams(matches)
    getTeamSquads(matches, players, "Liverpool")
    getFixtureOfATeam(matches, "Chelsea")
    joinQuery(matches, players, "Liverpool","Mohamed Salah")
    getMatchesAwayTeamBelowxG(matches, "Tottenham")
    callPython()

  }

}
