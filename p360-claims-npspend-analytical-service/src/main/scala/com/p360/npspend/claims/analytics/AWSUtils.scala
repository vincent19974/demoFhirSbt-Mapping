package com.p360.npspend.claims.analytics

import java.io.{ByteArrayOutputStream, IOException, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.apache.spark.internal.Logging
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.io.{BufferedSource, Source}
import scala.sys.process._

object AWSUtils extends Logging {

  def getJsonConfigasMap(confPath: String): Map[String, Any] = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val conigFileString: String = getS3ObjectAsString(confPath)
    parse(conigFileString.mkString).extract[Map[String, Any]]
  }

  def getBucketNameAndKey(s3Path: String): (String, String) = {
    //Function to split s3://path/to/key to bucket, path/to/key format
    val (bucketName, key) = s3Path.replace("s3://", "").span {
      _ != '/'
    }
    val fileKey = key.drop(1)
    //logInfo("Bucket name : " + bucketName + " and Key is : " + fileKey)
    (bucketName, fileKey)
  }

  def getS3ObjectAsString(fileName: String): String = {
    //Function to read s3 object and return as string
    val (bucketName, key) = getBucketNameAndKey(fileName)
    val amazonS3Client: AmazonS3 = AmazonS3ClientBuilder.standard.withRegion("us-east-1").build()
    val s3Object = amazonS3Client.getObject(bucketName, key)
    val source: BufferedSource = Source.fromInputStream(s3Object.getObjectContent)
    try {
      return source.mkString
    } catch {
      case e: IOException =>
    } finally {
      source.close()
      if (s3Object != null) s3Object.close()
    }
    ""
  }
  def runShellCmd(cmdType: String, source: String, destination: String): (Int, String, String) = {
    //Function to execute shell commands
    var cmd: Seq[String] = List()
    if (cmdType == "aws-cp") {
      cmd = cmd ++ Seq("aws", "s3", "cp", source, destination, "--recursive", "--sse", "AES256")
    }
    else if (cmdType == "aws-mv") {
      cmd = cmd ++ Seq("aws", "s3", "mv", source, destination, "--recursive", "--sse", "AES256")
    }
    else if (cmdType == "aws-rm") {
      cmd = cmd ++ Seq("aws", "s3", "rm", source, "--recursive")
    }
    else if (cmdType == "emrfs") {
      cmd = cmd ++ Seq("emrfs", "sync", source, "-m", destination)
    }
    logInfo("Executing Command : " + cmd)
    val stdOut = new ByteArrayOutputStream
    val stdErr = new ByteArrayOutputStream
    val stdOutWriter = new PrintWriter(stdOut)
    val stdErrWriter = new PrintWriter(stdErr)
    var exitValue = cmd.!(ProcessLogger(stdOutWriter.println, stdErrWriter.println))
    stdOutWriter.close()
    stdErrWriter.close()
    logInfo("Shell command return value is: " + exitValue)
    logInfo("Shell command STDOUT is:" + stdOut.toString)
    logInfo("Shell command STDERR value is: " + stdErr.toString)
    (exitValue, stdOut.toString, stdErr.toString)
  }

}
