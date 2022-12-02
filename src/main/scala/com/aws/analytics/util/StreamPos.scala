package com.aws.analytics.util

object StreamPos extends Enumeration {

  type StreamPos = Value
  val LATEST, TRIM_HORIZON, AT_TIMESTAMP = Value
}
