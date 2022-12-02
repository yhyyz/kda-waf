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

}
