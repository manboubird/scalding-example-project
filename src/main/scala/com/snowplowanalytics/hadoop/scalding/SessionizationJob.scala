package com.snowplowanalytics.hadoop.scalding

import ch.sentric.URL
//import ua_parser.Parser;
//import ua_parser.Client;
import com.github.nscala_time.time.Imports.DateTime
import com.twitter.scalding._
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat
import java.net.MalformedURLException

import scala.util.matching.Regex

/*
 * sample file download page: http://docs.splunk.com/Documentation/Splunk/5.0.7/Tutorial/GetthesampledataintoSplunk
 *
 * scald.rb --local ./src/main/scala/com/snowplowanalytics/hadoop/scalding/SessionizationJob.scala --input ./data/apache1.splunk.com/access_combined.log --output /tmp/scalding/SessionizationJob
 *
 */
class SessionizationJob(args : Args) extends Job(args) {

  val APACHE_LOG_PATTERN:Regex = """^([^ ]*) ([^ ]*) ([^ ]*) \[([^]]*)\] "([^ ]*)(?: *([^ ]*) *([^ ]*))?" ([^ ]*) ([^ ]*) "(.*?)" "(.*?)".*$""".r
  val JSESSIONID_PATTERN:Regex = """^.+JSESSIONID=([0-9A-Z]+).*$""".r
//  val LOG_SCHEMA = ('ip, 'ts, 'epoch, 'request_path, 'referrer, 'ua, 'sid, 'dev)
  val LOG_SCHEMA = ('sid, 'ts, 'path, 'referrer, 'epoch)
  val input = TextLine(args("input"))
//  val outputTrap = Tsv("trapped")
  val output = Tsv(args("output"))

  val in = input.read
    .mapTo('line -> LOG_SCHEMA){ line:String =>
      val matched = APACHE_LOG_PATTERN.findFirstMatchIn(line)
      matched match {
        case Some(m) =>
          val ip = m.group(1)
          val ts = m.group(4)
          val epoch:Long = toEpoch(ts)
          val requestPath = m.group(6)
          val referrer:String = m.group(10)
          val ua = m.group(11)
          //      val client:Client = new Parser().parse(ua)
          //      val dev = client.userAgent.familly + " " + client.userAgent.major + " " + client.userAgent.minor

          val sid = referrer match {
            case JSESSIONID_PATTERN(group) => group
            case _ => "test"
          }
          try {
            val referrerUrl = new URL(referrer)
            val referrerPath = referrerUrl.getPath.getAsString
            val path = new URL(referrerUrl.getScheme + "://" + referrerUrl.getAuthority.getHostName.getAsString + requestPath).getPath.getAsString
            (sid, ts, path, referrerPath, epoch)
          }catch{
            case e:java.net.MalformedURLException =>
              val path = ""
              val referrerPath = ""
              (sid, ts, path, referrerPath, epoch)
          }
        case None =>
          ("", "", "", "", "")
      }
//      (ip, ts, epoch, request, referrer, ua, sid, dev)
    }
    //    .addTrap(outputTrap)
    .insert('tmp, 0L)
    .insert('pvBySessionTmp, 0L)
    .insert('pseudoReferrerTmp, "")
    .groupBy('sid){
      _.sortBy('epoch)
        .scanLeft(('epoch, 'tmp, 'pvBySessionTmp, 'path ,'pseudoReferrerTmp)->('buffer, 'duration, 'pvBySession,'bufferPath, 'pseudoReferrer))((0L, 0L, 0L, "", "")) {
        (buffer: (Long, Long, Long, String, String), current:(Long, Long, Long, String, String)) =>
          val bufferedEpoch = buffer._1
          val lastPvBySession = buffer._3
          val epoch = current._1
          val duration = epoch - bufferedEpoch
          val pvBySession = if(duration > 30 * 60) 1 else lastPvBySession + 1

          val bufferedPath = buffer._4
          val pseudoReferrer = if(bufferedPath=="") "-" else bufferedPath
          val path = current._4

          (epoch, duration, pvBySession, path, pseudoReferrer)
      }.reducers(3)
    }
    .filter('duration, 'path) { x: (Long, String) => x._1 != 0 && x._2 != "" }
//    .project('sid, 'ts, 'epoch, 'pvBySession, 'duration, 'path, 'referrer)
    .project('sid, 'ts, 'pvBySession, 'path,'pseudoReferrer)
    .debug
    .map(('sid, 'ts, 'path, 'pseudoReferrer) -> 'json) {
      buffer:(String, String, String, String) =>
        val sid = buffer._1
        val ts = buffer._2
        val path = buffer._3
        val referrer = buffer._4
        (new Session(sid, ts, path, referrer).toJson)
    }
    .project('sid, 'json)
    .groupBy('sid) { _.mkString('json, ",") }
    .groupAll { _.mkString('json, "],[") }
    .write(output)

  case class Session(val sid:String, val time:String, val path:String, val referer:String){
    import com.google.gson.Gson
    def toJson():String =  new Gson().toJson(this)
  }

  val timezone = DateTimeZone.forID("America/Los_Angeles")
  val dateFormat = "dd/MMM/yyyy:HH:mm:ss"
  val fmt = DateTimeFormat.forPattern(dateFormat).withZone(timezone)
  def toEpoch(dateTime: String) = {
    fmt.parseDateTime(dateTime).getMillis / 1000
  }
}
