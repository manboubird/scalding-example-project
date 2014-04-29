package com.snowplowanalytics.hadoop.scalding

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.twitter.scalding.FieldConversions
import com.twitter.scalding.JobTest
import com.twitter.scalding.Tsv
import com.twitter.scalding.TupleConversions

@RunWith(classOf[JUnitRunner])
class TenDaysContinuousRateTest extends Specification with TupleConversions with FieldConversions {

  val inputFileName = "input.tsv"

  "Ten days continuous rate job" should {
    val inputList = List(
      // session id, epoch timestamp, search queries
      ("s1", "0", "q"), 		// s1, 01-01
      ("s1", "100000", "q"),	// s1, 01-02
      ("s1", "100000", "q2"),// s1, 01-02
      ("s2", "200000", "q"),	// s2, 01-03
      ("s2", "300000", "q"),	// s2, 01-04
      ("s3", "200000", "q"),	// s3, 01-03
      ("s4", "0", "q"),	    // s4, 01-01
      ("s4", "200000", "q")  // s4, 01-03
    )
    JobTest("com.snowplowanalytics.hadoop.scalding.TenDaysContinuousRate").
      arg("input", inputFileName).
      source(Tsv(inputFileName, ('sid, 'ts, 'queries)), inputList)
      .sink[(String,String,String,String)](Tsv("outputContinuousVisits")){
      output =>
        "count the number of sessions each first visit session cohort" in {
          output.size must_== 5
          output.toList must be_==(List(
            ("01-01","01-01","0","2"),
            ("01-01","01-02","1","1"),
            ("01-01","01-03","2","1"),
            ("01-03","01-03","0","2"),
            ("01-03","01-04","1","1")
          ))
        }
    }
      .run
      .finish
  }
}


