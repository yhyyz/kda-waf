package com.aws.analytics.util

import com.aws.analytics.model.ParamsModel
import org.apache.flink.api.java.utils.ParameterTool

import java.util
import java.util.Properties

object ParameterToolUtils {


  def fromApplicationProperties(properties: Properties): ParameterTool = {
    val map = new util.HashMap[String, String](properties.size())
    properties.forEach((k, v) => map.put(String.valueOf(k), String.valueOf(v)))
    ParameterTool.fromMap(map)
  }

  def genParams(parameter: ParameterTool): ParamsModel.Params = {
    val projectEnv = parameter.get("project_env")
    val awsRegion = parameter.get("aws_region")
    val ak = parameter.get("ak")
    val sk = parameter.get("sk")
    val inputStreamName = parameter.get("input_stream_name")
    val streamInitPosition = parameter.get("stream_init_position")
    val streamInitialTimestamp = parameter.get("stream_initial_timestamp")
    val targetStreamName = parameter.get("target_stream_name")
    val windowSize = parameter.get("window_size")
    val triggerValue = parameter.get("trigger_value")
    val params = ParamsModel.Params.apply(projectEnv,awsRegion,ak,sk,inputStreamName, streamInitPosition, streamInitialTimestamp, targetStreamName,windowSize,triggerValue)
    params
  }

  def genLookupJoinParams(parameter: ParameterTool): ParamsModel.LookupJoinParams = {
    val projectEnv = parameter.get("project_env")
    val awsRegion = parameter.get("aws_region")
    val ak = parameter.get("ak")
    val sk = parameter.get("sk")
    val inputStreamName = parameter.get("input_stream_name")
    val streamInitPosition = parameter.get("stream_init_position")
    val streamInitialTimestamp = parameter.get("stream_initial_timestamp")
    val targetStreamName = parameter.get("target_stream_name")
    val windowSize = parameter.get("window_size")
    val mysqlHost=parameter.get("mysql_host")
    val mysqlUser=parameter.get("mysql_user")
    val mysqlPassword=parameter.get("mysql_password")
    val mysqlTable=parameter.get("mysql_table")
    val cacheTTL = parameter.get("cache_ttl")
    val cacheMaxrows = parameter.get("cache_maxrows")
    val params = ParamsModel.LookupJoinParams.apply(projectEnv,awsRegion,ak,sk,inputStreamName, streamInitPosition, streamInitialTimestamp,
      targetStreamName,windowSize,mysqlHost,mysqlUser,mysqlPassword,mysqlTable,cacheTTL,cacheMaxrows)
    params
  }

  def genTemporaryJoinParams(parameter: ParameterTool): ParamsModel.TemporaryJoinParams = {
    val projectEnv = parameter.get("project_env")
    val awsRegion = parameter.get("aws_region")
    val ak = parameter.get("ak")
    val sk = parameter.get("sk")
    val inputStreamName = parameter.get("input_stream_name")
    val streamInitPosition = parameter.get("stream_init_position")
    val streamInitialTimestamp = parameter.get("stream_initial_timestamp")
    val targetStreamName = parameter.get("target_stream_name")
    val windowSize = parameter.get("window_size")
    val dimStreamName=parameter.get("dim_stream_name")
    val dimStreamInitPosition=parameter.get("dim_stream_init_position")
    val params = ParamsModel.TemporaryJoinParams.apply(projectEnv,awsRegion,ak,sk,inputStreamName, streamInitPosition, streamInitialTimestamp,
      targetStreamName,windowSize,dimStreamName,dimStreamInitPosition)
    params
  }

  def genHOPTemporaryJoinParams(parameter: ParameterTool): ParamsModel.HOPTemporaryJoinParams = {
    val projectEnv = parameter.get("project_env")
    val awsRegion = parameter.get("aws_region")
    val ak = parameter.get("ak")
    val sk = parameter.get("sk")
    val inputStreamName = parameter.get("input_stream_name")
    val streamInitPosition = parameter.get("stream_init_position")
    val streamInitialTimestamp = parameter.get("stream_initial_timestamp")
    val targetStreamName = parameter.get("target_stream_name")
    val windowSize = parameter.get("window_size")
    val windowInterval = parameter.get("window_interval")
    val dimStreamName=parameter.get("dim_stream_name")
    val dimStreamInitPosition=parameter.get("dim_stream_init_position")
    val params = ParamsModel.HOPTemporaryJoinParams.apply(projectEnv,awsRegion,ak,sk,inputStreamName, streamInitPosition, streamInitialTimestamp,
      targetStreamName,windowSize,windowInterval,dimStreamName,dimStreamInitPosition)
    params
  }


}
