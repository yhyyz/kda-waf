package com.aws.analytics.model

object ParamsModel {

  case class Params(projectEnv:String ,awsRgeion: String, ak:String,sk:String, inputStreamName: String, streamInitPosition: String, streamInitialTimestamp: String, targetStreamName: String,windowSize:String,triggerValue:String)
}
