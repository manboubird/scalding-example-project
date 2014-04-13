/*
 * Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
import sbt._

object Dependencies {
  val resolutionRepos = Seq(
    "ScalaTools snapshots at Sonatype" at "https://oss.sonatype.org/content/repositories/snapshots/",
    "Concurrent Maven Repo" at "http://conjars.org/repo", // For Scalding, Cascading etc
    "cloudera-releases" at "https://repository.cloudera.com/artifactory/cloudera-repos"
//    ,"Local Repository" at file(Path.userHome.absolutePath + "/.ivy2/local").getAbsolutePath
  )

  object V {
    val scalding  = "0.10.0-mr1-cdh4.5.0"
    val hadoopCore    = "2.0.0-mr1-cdh4.5.0"
    val hadoopCommon = "2.0.0-cdh4.5.0"
    val hadoopClient = "2.0.0-mr1-cdh4.5.0"
    val specs2    = "2.3.11"//"1.12.3" // -> "1.13" when we bump to Scala 2.10.0
    // Add versions for your additional libraries here...
  }

  object Libraries {
    val scaldingCore = "com.twitter"                %% "scalding-core"        % V.scalding
    val hadoopCore   = "org.apache.hadoop"          %  "hadoop-core"          % V.hadoopCore   % "provided"
    // Add additional libraries from mvnrepository.com (SBT syntax) here...
    val hadoopCommon = "org.apache.hadoop"          %  "hadoop-common"        % V.hadoopCommon % "provided"
    val hadoopClient = "org.apache.hadoop"          %  "hadoop-client"        % V.hadoopClient % "provided"
    // Scala (test only)
    val specs2       = "org.specs2"                 %% "specs2"               % V.specs2       % "test"
  }
}
