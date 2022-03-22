package com.conviva.platform

import java.util
import java.util.Map

import com.conviva.clickhouse.ContentSessionHulu
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.log4j.Logger
import org.joda.time.DateTime

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object HULUSessionParser {
  val DEFAULT_SEPARATOR = '\t'
  val DEFAULT_LINE_END = "\n"
  val quote = "'"
  val tag_separator_old="\\."
  val tag_separator_new="_"

  val numBucketsPerPeriod = 60
  val encodedBucketBits = Integer.SIZE - Integer.numberOfLeadingZeros(numBucketsPerPeriod)
  val encodedBucketMask = (1 << encodedBucketBits) - 1


  private def peekLastBucket(encodedIntervals: Long): Int = encodedIntervals.toInt & encodedBucketMask

  private def removeLastBucket(encodedIntervals: Long): Long = encodedIntervals >> encodedBucketBits


  def decodePcp(encodedIntervals: Long): String = {
    val result = new ListBuffer[Int]()
    var flag = encodedIntervals
    while (flag != 0) {
      val endBucket = peekLastBucket(flag)
      flag = removeLastBucket(flag)
      val startBucket = peekLastBucket(flag)
      flag = removeLastBucket(flag)
      for (play_minute <- startBucket until endBucket) {
        result += play_minute
      }
    }
    val array = result.toList.toArray

    val ret = array.mkString("[", ",", "]")
    ret

  }

  def decodePcpWithStartEnd(encodedIntervals: Long): (String,String) = {
    val startList = new ListBuffer[Int]()

    val endList = new ListBuffer[Int]()
    var flag = encodedIntervals
    while (flag != 0) {
      val endBucket = peekLastBucket(flag)
      flag = removeLastBucket(flag)
      val startBucket = peekLastBucket(flag)
      flag = removeLastBucket(flag)
//      for (play_minute <- startBucket until endBucket) {
//        result += play_minute
//      }
      startList+=startBucket
      ///   if (endBucket < numBucketsPerPeriod) {
      //                bucketDeltas[endBucket]--;
      //            }
      // should not contain minute 60 for pcp sum
      if (endBucket < numBucketsPerPeriod) endList+=endBucket
    }
    val startarray = startList.toList.toArray
    val endarray = endList.toList.toArray

    val startret = startarray.mkString("[", ",", "]")
    val endret = endarray.mkString("[", ",", "]")
    (startret,endret)

  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("hulu-reader")

      .master("local[12]").getOrCreate()

        val inputpath = args(0)
        val outputpath = args(1)

//    val inputpath = "hulu.txt"
//    val outputpath = "a.tsv"

    val data = spark.read.textFile(inputpath).rdd
    data.map(parseSession(_)).saveAsTextFile(outputpath)

  }

  def buildStringArray(str: String) = str.replaceAll(quote, "").replaceAll("\"", "\'")


  def parseSession(sessionJson: String): String = {
    val jsonTree = new ObjectMapper().readTree(sessionJson)


    val cs = new ContentSessionHulu()
    val tags = new java.util.HashMap[String, String]


    for (jn <- jsonTree.fields().asScala) {

      val valueStr = jn.getValue.asText.replaceAll(quote, "").replaceAll(DEFAULT_LINE_END, "").replaceAll("\t", "")
      val nodeName = jn.getKey
      if ("version" == nodeName) cs.setVersion(valueStr)
      else if ("customerId" == nodeName) cs.setCustomerId(valueStr.toInt)
      else if ("clientId" == nodeName) cs.setClientId(valueStr)
      else if ("sessionId" == nodeName) cs.setSessionId(valueStr.toInt)
      else if ("segmentId" == nodeName) cs.setSegmentId(valueStr.toInt)
      else if ("datasourceId" == nodeName) cs.setDatasourceId(valueStr)
      else if ("isAudienceOnly" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setIsAudienceOnly(i)
      }
      else if ("isAd" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setIsAd(i)
      }
      else if ("assetName" == nodeName) cs.setAssetName(valueStr.replaceAll(DEFAULT_LINE_END, "").replaceAll("\t", ""))
      else if ("streamUrl" == nodeName) cs.setStreamUrl(valueStr)
      else if ("contentLengthMs" == nodeName) cs.setContentLengthMs(valueStr.toInt)
      else if ("ipType" == nodeName) cs.setIpType(valueStr)
      else if ("geo.continent" == nodeName) cs.setGeo_continent(valueStr.toLong)
      else if ("geo.country" == nodeName) cs.setGeo_country(valueStr.toLong)
      else if ("geo.state" == nodeName) cs.setGeo_state(valueStr.toLong)
      else if ("geo.city" == nodeName) cs.setGeo_city(valueStr.toLong)
      else if ("geo.dma" == nodeName) cs.setGeo_dma(valueStr.toInt)
      else if ("geo.asn" == nodeName) cs.setGeo_asn(valueStr.toInt)
      else if ("geo.isp" == nodeName) cs.setGeo_isp(valueStr.toInt)
      else if ("geo.postalCode" == nodeName) cs.setGeo_postalCode(valueStr)
      else if ("life.firstReceivedTimeMs" == nodeName) cs.setLife_firstReceivedTimeMs(valueStr.toLong)
      else if ("life.latestReceivedTimeMs" == nodeName) cs.setLife_latestReceivedTimeMs(valueStr.toLong)
      else if ("life.sessionTimeMs" == nodeName) cs.setLife_sessionTimeMs(valueStr.toInt)
      else if ("life.joinTimeMs" == nodeName) cs.setLife_joinTimeMs(valueStr.toInt)
      else if ("life.playingTimeMs" == nodeName) cs.setLife_playingTimeMs(valueStr.toInt)
      else if ("life.bufferingTimeMs" == nodeName) cs.setLife_bufferingTimeMs(valueStr.toInt)
      else if ("life.networkBufferingTimeMs" == nodeName) cs.setLife_networkBufferingTimeMs(valueStr.toInt)
      else if ("life.rebufferingRatioPct" == nodeName) cs.setLife_rebufferingRatioPct(valueStr.toFloat)
      else if ("life.networkRebufferingRatioPct" == nodeName) cs.setLife_networkRebufferingRatioPct(valueStr.toFloat)
      else if ("life.averageBitrateKbps" == nodeName) cs.setLife_averageBitrateKbps(valueStr.toInt)
      else if ("life.seekJoinTimeMs" == nodeName) cs.setLife_seekJoinTimeMs(valueStr.toInt)
      else if ("life.seekJoinCount" == nodeName) cs.setLife_seekJoinCount(valueStr.toInt)
      else if ("life.bufferingEvents" == nodeName) cs.setLife_bufferingEvents(valueStr.toInt)
      else if ("life.networkRebufferingEvents" == nodeName) cs.setLife_networkRebufferingEvents(valueStr.toInt)
      else if ("life.bitrateKbps" == nodeName) cs.setLife_bitrateKbps(valueStr.toInt)
      else if ("life.contentWatchedTimeMs" == nodeName) cs.setLife_contentWatchedTimeMs(valueStr.toInt)
      else if ("life.contentWatchedPct" == nodeName) cs.setLife_contentWatchedPct(valueStr.toFloat)
      else if ("life.averageFrameRate" == nodeName) cs.setLife_averageFrameRate(valueStr.toInt)
      else if ("life.renderingQuality" == nodeName) cs.setLife_renderingQuality(valueStr.toInt)
      else if ("life.resourceIds" == nodeName) cs.setLife_resourceIds(jn.getValue.toString)
      else if ("life.cdns" == nodeName) cs.setLife_cdns(buildStringArray(jn.getValue.toString))
      else if ("life.fatalErrorResourceIds" == nodeName) cs.setLife_fatalErrorResourceIds(jn.getValue.toString)
      else if ("life.fatalErrorCdns" == nodeName) cs.setLife_fatalErrorCdns(buildStringArray(jn.getValue.toString))
      else if ("life.latestErrorResourceId" == nodeName) cs.setLife_latestErrorResourceId(valueStr.toInt)
      else if ("life.latestErrorCdn" == nodeName) cs.setLife_latestErrorCdn(valueStr)
      else if ("life.joinResourceIds" == nodeName) {
        val aaa = jn.getValue.toString
        cs.setLife_joinResourceIds(jn.getValue.toString)
      }
      else if ("life.joinCdns" == nodeName) cs.setLife_joinCdns(buildStringArray(jn.getValue.toString))
      else if ("life.lastJoinCdn" == nodeName) cs.setLife_lastJoinCdn(valueStr)
      else if ("life.lastCdn" == nodeName) cs.setLife_lastCdn(valueStr)
      else if ("life.lastJoinResourceId" == nodeName) cs.setLife_lastJoinResourceId(valueStr.toInt)
      else if ("life.isVideoPlaybackFailure" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setLife_isVideoPlaybackFailure(i)
      }
      else if ("life.isVideoStartFailure" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setLife_isVideoStartFailure(i)
      }
      else if ("life.hasJoined" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setLife_hasJoined(i)
      }
      else if ("life.isVideoPlaybackFailureBusiness" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setLife_isVideoPlaybackFailureBusiness(i)
      }
      else if ("life.isVideoPlaybackFailureTech" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setLife_isVideoPlaybackFailureTech(i)
      }
      else if ("life.isVideoStartFailureBusiness" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setLife_isVideoStartFailureBusiness(i)
      }
      else if ("life.isVideoStartFailureTech" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setLife_isVideoStartFailureTech(i)
      }
      else if ("life.videoPlaybackFailureErrorsBusiness" == nodeName) cs.setLife_videoPlaybackFailureErrorsBusiness(buildStringArray(jn.getValue.toString))
      else if ("life.videoPlaybackFailureErrorsTech" == nodeName) cs.setLife_videoPlaybackFailureErrorsTech(buildStringArray(jn.getValue.toString))
      else if ("life.videoStartFailureErrorsBusiness" == nodeName) cs.setLife_videoStartFailureErrorsBusiness(buildStringArray(jn.getValue.toString))
      else if ("life.videoStartFailureErrorsTech" == nodeName) cs.setLife_videoStartFailureErrorsTech(buildStringArray(jn.getValue.toString))
      else if ("life.exitDuringPreRoll" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setLife_exitDuringPreRoll(i)
      }
      else if ("life.waitTimePrerollExitMs" == nodeName) cs.setLife_waitTimePrerollExitMs(valueStr.toInt)
      else if ("life.lastCDNGroupId" == nodeName) cs.setLife_lastCDNGroupId(valueStr)
      else if ("life.lastCDNEdgeServer" == nodeName) cs.setLife_lastCDNEdgeServer(valueStr)
      else if ("interval.startTimeMs" == nodeName) cs.setInterval_startTimeMs(valueStr.toLong)
      else if ("switch.resourceId" == nodeName) cs.setSwitch_resourceId(valueStr.toInt)
      else if ("switch.cdn" == nodeName) cs.setSwitch_cdn(valueStr)
      else if ("switch.justJoined" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setSwitch_justJoined(i)
      }
      else if ("switch.hasJoined" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setSwitch_hasJoined(i)
      }
      else if ("switch.justJoinedAndLifeJoinTimeMsIsAccurate" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setSwitch_justJoinedAndLifeJoinTimeMsIsAccurate(i)
      }
      else if ("switch.isEndedPlay" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setSwitch_isEndedPlay(i)
      }
      else if ("switch.isEnded" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setSwitch_isEnded(i)
      }
      else if ("switch.isEndedPlayAndLifeAverageBitrateKbpsGT0" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setSwitch_isEndedPlayAndLifeAverageBitrateKbpsGT0(i)
      }
      else if ("switch.isVideoStartFailure" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setSwitch_isVideoStartFailure(i)
      }
      else if ("switch.videoStartFailureErrors" == nodeName) cs.setSwitch_videoStartFailureErrors(buildStringArray(jn.getValue.toString))
      else if ("switch.isExitBeforeVideoStart" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setSwitch_isExitBeforeVideoStart(i)
      }
      else if ("switch.isVideoPlaybackFailure" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setSwitch_isVideoPlaybackFailure(i)
      }
      else if ("switch.isVideoStartSave" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setSwitch_isVideoStartSave(i)
      }
      else if ("switch.videoPlaybackFailureErrors" == nodeName) cs.setSwitch_videoPlaybackFailureErrors(buildStringArray(jn.getValue.toString))
      else if ("switch.isAttempt" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setSwitch_isAttempt(i)
      }
      else if ("switch.playingTimeMs" == nodeName) cs.setSwitch_playingTimeMs(valueStr.toInt)
      else if ("switch.rebufferingTimeMs" == nodeName) cs.setSwitch_rebufferingTimeMs(valueStr.toInt)
      else if ("switch.networkRebufferingTimeMs" == nodeName) cs.setSwitch_networkRebufferingTimeMs(valueStr.toInt)
      else if ("switch.rebufferingDuringAdsMs" == nodeName) cs.setSwitch_rebufferingDuringAdsMs(valueStr.toInt)
      else if ("switch.adRelatedBufferingMs" == nodeName) cs.setSwitch_adRelatedBufferingMs(valueStr.toInt)
      else if ("switch.bitrateBytes" == nodeName) cs.setSwitch_bitrateBytes(valueStr.toLong)
      else if ("switch.bitrateTimeMs" == nodeName) cs.setSwitch_bitrateTimeMs(valueStr.toInt)
      else if ("switch.framesLoaded" == nodeName) cs.setSwitch_framesLoaded(valueStr.toInt)
      else if ("switch.framesPlayingTimeMs" == nodeName) cs.setSwitch_framesPlayingTimeMs(valueStr.toInt)
      else if ("switch.seekJoinTimeMs" == nodeName) cs.setSwitch_seekJoinTimeMs(valueStr.toInt)
      else if ("switch.seekJoinCount" == nodeName) cs.setSwitch_seekJoinCount(valueStr.toInt)
      else if ("switch.pcpBuckets1Min" == nodeName) cs.setSwitch_pcpBuckets1Min(jn.getValue.toString)
      else if ("switch.pcpIntervals" == nodeName) {
        cs.setSwitch_pcpIntervals(valueStr.toLong)
        val pcpBuckets1Min = decodePcp(valueStr.toLong)
        cs.setSwitch_pcpBuckets1Min(pcpBuckets1Min)

        val(startminutes,endminutes) = decodePcpWithStartEnd(valueStr.toLong)

        cs.setSwitch_pcpStartBuckets1Min(startminutes)
        cs.setSwitch_pcpEndBuckets1Min(endminutes)
      }
      else if ("switch.rebufferingTimeMsRaw" == nodeName) cs.setSwitch_rebufferingTimeMs(valueStr.toInt)
      else if ("switch.networkRebufferingTimeMsRaw" == nodeName) cs.setSwitch_networkRebufferingTimeMsRaw(valueStr.toInt)
      else if ("switch.isVideoPlaybackFailureBusiness" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setSwitch_isVideoPlaybackFailureBusiness(i)
      }
      else if ("switch.isVideoPlaybackFailureTech" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setLife_isVideoPlaybackFailureTech(i)
      }
      else if ("switch.isVideoStartFailureBusiness" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setSwitch_isVideoStartFailureBusiness(i)
      }
      else if ("switch.isVideoStartFailureTech" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setSwitch_isVideoStartFailureTech(i)
      }
      else if ("switch.videoPlaybackFailureErrorsBusiness" == nodeName) cs.setSwitch_videoPlaybackFailureErrorsBusiness(buildStringArray(jn.getValue.toString))
      else if ("switch.videoPlaybackFailureErrorsTech" == nodeName) cs.setSwitch_videoPlaybackFailureErrorsTech(buildStringArray(jn.getValue.toString))
      else if ("switch.videoStartFailureErrorsBusiness" == nodeName) cs.setSwitch_videoStartFailureErrorsBusiness(buildStringArray(jn.getValue.toString))
      else if ("switch.videoStartFailureErrorsTech" == nodeName) cs.setSwitch_videoStartFailureErrorsTech(buildStringArray(jn.getValue.toString))
      else if ("switch.adRequested" == nodeName) {
        val i = if ("true" == valueStr) 1
        else 0
        cs.setSwitch_adRequested(i)
      }
      else if ("bucket.sessionTimeMs" == nodeName) cs.setBucket_sessionTimeMs(valueStr.toInt)
      else if ("bucket.joinTimeMs" == nodeName) cs.setBucket_joinTimeMs(valueStr.toInt)
      else if ("bucket.playingTimeMs" == nodeName) cs.setBucket_playingTimeMs(valueStr.toInt)
      else if ("bucket.bufferingTimeMs" == nodeName) cs.setBucket_bufferingTimeMs(valueStr.toInt)
      else if ("bucket.networkBufferingTimeMs" == nodeName) cs.setBucket_networkBufferingTimeMs(valueStr.toInt)
      else if ("bucket.rebufferingRatioPct" == nodeName) cs.setBucket_rebufferingRatioPct(valueStr.toFloat)
      else if ("bucket.networkRebufferingRatioPct" == nodeName) cs.setBucket_networkRebufferingRatioPct(valueStr.toFloat)
      else if ("bucket.averageBitrateKbps" == nodeName) cs.setBucket_averageBitrateKbps(valueStr.toInt)
      else if ("bucket.seekJoinTimeMs" == nodeName) cs.setBucket_seekJoinTimeMs(valueStr.toInt)
      else if ("bucket.averageFrameRate" == nodeName) cs.setBucket_averageFrameRate(valueStr.toInt)
      else if ("bucket.contentWatchedPct" == nodeName) cs.setBucket_contentWatchedPct(valueStr.toFloat)
      else if ("tags.m3.dv.hwt" == nodeName) cs.setM3_dv_hwt(valueStr)
      else if ("tags.m3.dv.mrk" == nodeName) cs.setM3_dv_mrk(valueStr)
      else if ("tags.c3.video.isAd" == nodeName) {
        val i = if ("T" == valueStr) 1
        else 0
        cs.setC3_video_isAd(i)
      }
      else if ("tags.c3.ad.id" == nodeName) cs.setC3_ad_id(valueStr)
      else if ("tags.c3.ad.position" == nodeName) cs.setC3_ad_position(valueStr)
      else if ("tags.c3.ad.system" == nodeName) cs.setC3_ad_system(valueStr)
      else if ("tags.c3.ad.technology" == nodeName) cs.setC3_ad_technology(valueStr)
      else if ("tags.c3.ad.isSlate" == nodeName) cs.setC3_ad_isSlate(valueStr)
      else if ("tags.c3.ad.adStitcher" == nodeName) cs.setC3_ad_adStitcher(valueStr)
      else if ("tags.c3.ad.creativeId" == nodeName) cs.setC3_ad_creativeId(valueStr)
      else if ("tags.c3.ad.breakId" == nodeName) cs.setC3_ad_breakId(valueStr)
      else if ("tags.c3.ad.contentAssetName" == nodeName) cs.setC3_ad_contentAssetName(valueStr)
      else if ("tags.c3.pt_ver" == nodeName) cs.setC3_pt_ver(valueStr)
      else if ("tags.c3.device_ua" == nodeName) cs.setC3_device_ua(valueStr)
      else if ("tags.c3.adaptor_type" == nodeName) cs.setC3_adaptor_type(valueStr)
      else if ("tags.c3.protocol.level" == nodeName) cs.setC3_protocol_level(valueStr)
      else if ("tags.c3.protocol.pure" == nodeName) cs.setC3_protocol_pure(valueStr)
      else if ("tags.c3.pt.os.ver" == nodeName) cs.setC3_pt_os_ver(valueStr)
      else if ("tags.c3.device.manufacturer" == nodeName) cs.setC3_device_manufacturer(valueStr)
      else if ("tags.c3.device.brand" == nodeName) cs.setC3_device_brand(valueStr)
      else if ("tags.c3.device.model" == nodeName) cs.setC3_device_model(valueStr)
      else if ("tags.c3.device.conn" == nodeName) cs.setC3_device_conn(valueStr)
      else if ("tags.c3.device.ver" == nodeName) cs.setC3_device_ver(valueStr)
      else if ("tags.c3.go.algoid" == nodeName) cs.setC3_go_algoid(valueStr)
      else if ("tags.c3.player.name" == nodeName) cs.setC3_player_name(valueStr)
      else if ("tags.c3.de.rs.raw" == nodeName) cs.setC3_de_rs_raw(valueStr)
      else if ("tags.c3.de.bitr" == nodeName) cs.setC3_de_bitr(valueStr)
      else if ("tags.c3.de.rsid" == nodeName) cs.setC3_de_rsid(valueStr)
      else if ("tags.c3.de.cdn" == nodeName) cs.setC3_de_cdn(valueStr)
      else if ("tags.c3.de.rs" == nodeName) cs.setC3_de_rs(valueStr)
      else if ("tags.c3.device.cver" == nodeName) cs.setC3_device_cver(valueStr)
      else if ("tags.c3.device.cver.bld" == nodeName) cs.setC3_device_cver_bld(valueStr)
      else if ("tags.c3.video.isLive" == nodeName) {
        val i = if ("T" == valueStr) 1
        else 0
        cs.setC3_video_isLive(i)
      }
      else if ("tags.c3.viewer.id" == nodeName) cs.setC3_viewer_id(valueStr)
      else if ("tags.c3.client.hwType" == nodeName) cs.setC3_client_hwType(valueStr)
      else if ("tags.c3.client.osname" == nodeName) cs.setC3_client_osname(valueStr)
      else if ("tags.c3.client.manufacturer" == nodeName) cs.setC3_client_manufacturer(valueStr)
      else if ("tags.c3.client.brand" == nodeName) cs.setC3_client_brand(valueStr)
      else if ("tags.c3.client.marketingName" == nodeName) cs.setC3_client_marketingName(valueStr)
      else if ("tags.c3.client.model" == nodeName) cs.setC3_client_model(valueStr)
      else if ("tags.c3.client.osv" == nodeName) cs.setC3_client_osv(valueStr)
      else if ("tags.c3.client.osf" == nodeName) cs.setC3_client_osf(valueStr)
      else if ("tags.c3.client.br" == nodeName) cs.setC3_client_br(valueStr)
      else if ("tags.c3.client.brv" == nodeName) cs.setC3_client_brv(valueStr)
      else if ("tags.m3.dv.mnf" == nodeName) cs.setM3_dv_mnf(valueStr)
      else if ("tags.m3.dv.n" == nodeName) cs.setM3_dv_n(valueStr)
      else if ("tags.m3.dv.os" == nodeName) cs.setM3_dv_os(valueStr)
      else if ("tags.m3.dv.osv" == nodeName) cs.setM3_dv_osv(valueStr)
      else if ("tags.m3.dv.osf" == nodeName) cs.setM3_dv_osf(valueStr)
      else if ("tags.m3.dv.br" == nodeName) cs.setM3_dv_br(valueStr)
      else if ("tags.m3.dv.brv" == nodeName) cs.setM3_dv_brv(valueStr)
      else if ("tags.m3.dv.fw" == nodeName) cs.setM3_dv_fw(valueStr)
      else if ("tags.m3.dv.fwv" == nodeName) cs.setM3_dv_fwv(valueStr)
      else if ("tags.m3.dv.mod" == nodeName) cs.setM3_dv_mod(valueStr)
      else if ("tags.m3.dv.vnd" == nodeName) cs.setM3_dv_vnd(valueStr)
      else if ("tags.m3.net.t" == nodeName) cs.setM3_net_t(valueStr)
      else if ("tags.c3.protocol.type" == nodeName) cs.setC3_protocol_type(valueStr)
      else if ("tags.c3.device.type" == nodeName) cs.setC3_device_type(valueStr)
      else if ("tags.c3.pt.os" == nodeName) cs.setC3_pt_os(valueStr)
      else if ("tags.c3.ft.os" == nodeName) cs.setC3_ft_os(valueStr)
      else if ("tags.c3.framework" == nodeName) cs.setC3_framework(valueStr)
      else if ("tags.c3.framework.ver" == nodeName) cs.setC3_framework_ver(valueStr)
      else if ("tags.c3.pt.br" == nodeName) cs.setC3_pt_br(valueStr)
      else if ("tags.c3.pt.br.ver" == nodeName) cs.setC3_pt_br_ver(valueStr)
      else if ("tags.c3_br.v" == nodeName) cs.setC3_br_v(valueStr)
      else if ("tags.appVersion" == nodeName) cs.setAppVersion(valueStr)
      else if ("tags.network" == nodeName) cs.setNetwork(valueStr)
      else if ("tags.stormflowId" == nodeName) cs.setStormflowId(valueStr)
      else if ("tags.contentType" == nodeName) cs.setContentType(valueStr)
      else if ("tags.m3.dv.cat" == nodeName) cs.setM3_dv_cat(valueStr)
      else if ("tags.c3.cp.an" == nodeName) cs.setC3_cp_an(valueStr)
      else if ("tags.huluPlayerFrameworkName" == nodeName) cs.setHuluPlayerFrameworkName(valueStr)
      else if ("tags.liveSignalProvider" == nodeName) cs.setLiveSignalProvider(valueStr)
      else if ("tags.clientFeatureTags" == nodeName) cs.setClientFeatureTags(valueStr)
      else if ("tags.channel" == nodeName) cs.setChannel(valueStr)
      else if ("tags.huluPlayerFrameworkVersion" == nodeName) cs.setHuluPlayerFrameworkVersion(valueStr)
      else if ("tags.startType" == nodeName) cs.setStartType(valueStr)
      else if ("tags.plt" == nodeName) cs.setPlt(valueStr)
      else if ("tags.DevOps" == nodeName) cs.setDevOps(valueStr)
      else if ("tags.conid" == nodeName) cs.setConid(valueStr)
      else if ("tags.stormflow" == nodeName) cs.setStormflow(valueStr)
      else if (nodeName.startsWith("tags.")) {
        val key = quote + nodeName.substring(5).replaceAll(tag_separator_old,tag_separator_new) + quote
        tags.put(key, (quote + valueStr.replaceAll(quote, "") + quote).replaceAll(DEFAULT_LINE_END, ""))
      }
      else {
        //                        logger.warn(">>> unknown field:" + jn.toString());
      }


    }

    cs.setTags(tags);
    cs.toSparkTSV
  }


}
