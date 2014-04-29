package com.snowplowanalytics.hadoop.scalding

import com.github.nscala_time.time.Imports.DateTime
import com.twitter.scalding.Args
import com.twitter.scalding.Job
import com.twitter.scalding.Tsv
//
// scald.rb  ./src/main/scala/com/snowplowanalytics/hadoop/scalding/TenDaysContinuousRate.scala --input /user/hive/warehouse/excite_log/year=2000/*
//
// 検索ログから初回訪問日のコホートの継続利用セッション数を算出します
class TenDaysContinuousRate(args : Args) extends Job(args) {

  val input = Tsv(args("input"), ('sid, 'ts, 'queries))
  val outputTrap = Tsv("trapped")
  val outputContinuousVisits = Tsv("outputContinuousVisits")

  val in = input.read
    //    .addTrap(outputTrap) // 不正なログはTrapで除外します。
    .mapTo(('sid, 'ts, 'queries) -> ('sid, 'ts, 'queries, 'date)){
    v: (String, String, String) => {
      val (sid, ts, queries) = v
      // unixタイムスタンプを月日に変換してフィールドとして追加
      (sid, ts, queries, toDateFormat(getDate(ts)))
    }
  }
    // １、２月のログのみをフィルタリングします。
    .filter('date) { date : String => isTargetDate(date) }

  // セッション毎にタイムスタンプでソートし、初回訪問日のログ（タイムスタンプが最小のログ）を取得します。
  val firstVisit = in
    .groupBy('sid){ _.sortBy('ts).take(1) }
    .filter('date) { date : String => isTargetFirstVisitDate(date) }
    .rename(('sid, 'ts, 'queries, 'date) -> ('sid_fv, 'ts_fv, 'queries_fv, 'date_fv))

  // 初回訪問日のレコードと全体のログをジョインし、
  // 初回訪問以降の訪問日とその訪問日のセッションのユニーク数をカウント
  val continuousVisits = firstVisit
    .project('sid_fv, 'ts_fv, 'date_fv)
    .joinWithLarger('sid_fv -> 'sid, in, new cascading.pipe.joiner.InnerJoin)
    .map(('ts, 'ts_fv) -> 'past_days){ v : (String, String) =>
    val (ts, ts_fv) = v
    getDiffDays(ts_fv, ts)
  }
    .project('date_fv, 'date, 'past_days, 'sid)
    // 初回訪問日から10日以内の実績のみをフィルタリング
    .filter('past_days) { past_days : String => java.lang.Integer.parseInt(past_days) <= 10 }
    .unique('date_fv, 'date, 'past_days, 'sid)
    .groupBy('date_fv, 'date, 'past_days) {
    _.size('sid_fv_count_by_date)
  }
    .project('date_fv, 'date, 'past_days, 'sid_fv_count_by_date)

  continuousVisits.write(outputContinuousVisits)


  def getDate(date : String) : java.util.Date = {
    new java.util.Date(java.lang.Long.parseLong(date) * 1000);
  }

  def toDateFormat(date : java.util.Date) : String = {
    new java.text.SimpleDateFormat("MM-dd").format(date);
  }

  def getDiffDays(from : String, to: String): Int = {
    org.joda.time.Days.daysBetween(
      new DateTime(getDate(from)).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0),
      new DateTime(getDate(to)).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
    ).getDays();
  }

  def isTargetDate(date : String) : Boolean = {
    val p = """^(01|02)-\d{2}$""".r
    p.pattern.matcher(date).matches()
  }

  def isTargetFirstVisitDate(date : String) : Boolean = {
    val p = """^(01-\d{2}|02-0\d|02-10)$""".r
    p.pattern.matcher(date).matches()
  }
}
