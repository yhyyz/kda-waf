package com.aws.analytics.model

object ParamsModel {

  case class Params(projectEnv:String ,awsRgeion: String, ak:String,sk:String, inputStreamName: String, streamInitPosition: String, streamInitialTimestamp: String, targetStreamName: String,windowSize:String,triggerValue:String)
  case class LookupJoinParams(projectEnv:String ,awsRgeion: String, ak:String,sk:String, inputStreamName: String, streamInitPosition: String, streamInitialTimestamp: String,
                              targetStreamName: String,windowSize:String,
                              mysqlHost: String,mysqlUser:String,mysqlPassword:String,mysqlTable:String,cacheTTL:String,cacheMaxrows:String
                             )

  case class TemporaryJoinParams(projectEnv:String ,awsRgeion: String, ak:String,sk:String, inputStreamName: String, streamInitPosition: String, streamInitialTimestamp: String,
                              targetStreamName: String,windowSize:String,
                                 dimStreamName:String,dimStreamInitPosition:String
                             )

}
