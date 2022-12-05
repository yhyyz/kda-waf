/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aws.analytics

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
import com.aws.analytics.model.{ParamsModel}
import com.aws.analytics.util.{ParameterToolUtils, StreamPos}
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.kinesis.shaded.org.apache.flink.connector.aws.config.AWSConfigConstants
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.{LocalStreamEnvironment, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.flink.table.api.{AnyWithOperations, EnvironmentSettings, FieldExpression, Schema}
import org.apache.flink.table.expressions.ExpressionParserImpl.PROCTIME
import org.apache.flink.types.Row
import org.apache.logging.log4j.LogManager

/**
 * LookUp JOIN
 */
object StreamingLookupJoinWAF {
  private val log = LogManager.getLogger(StreamingLookupJoinWAF.getClass)

  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //      .disableOperatorChaining()
    // 注意在Kinesis Analysis 运行时中该参数不生效，需要在CLI中设置相关参数，同时KDA 默认会使用RocksDB存储状态，不用设置
    env.enableCheckpointing(5000)
    var parameter: ParameterTool = null
    if (env.getClass == classOf[LocalStreamEnvironment]) {
      parameter = ParameterTool.fromArgs(args)
    } else {
      // 使用KDA Runtime获取参数数
      val applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties.get("FlinkAppProperties")
      if (applicationProperties == null) {
        throw new RuntimeException("Unable to load properties from Group ID FlinkAppProperties.")
      }
      parameter = ParameterToolUtils.fromApplicationProperties(applicationProperties)
    }
    val params = ParameterToolUtils.genLookupJoinParams(parameter)
    log.info("waf-lookup-join-params" + params.toString)

    // 创建 table env，流模式
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    val conf = new Configuration()
    conf.setString("pipeline.name", "kda-waf-lookup-join");
    tEnv.getConfig.addConfiguration(conf)

    val source = createSourceFromStaticConfig(env, params)
    val data = source.map(line => {
      try {
        val dataArray = line.replaceAll("\n", "").split("\\\\t")
        Row.of(dataArray(0), dataArray(1))
      } catch {
        case e: Exception => {
          log.error(e.getMessage)
        }
          Row.of("error", "error")
      }
    }).returns(Types.ROW(Types.STRING, Types.STRING))

    val sourceTable = tEnv.fromDataStream(data, Schema.newBuilder()
      .columnByExpression("ts", "proctime()")
      .build())
      .as("ip", "url", "ts")
    tEnv.createTemporaryView("source_table", sourceTable)

    var credentialsProvider = "AUTO"
    if (params.projectEnv == "local") {
      credentialsProvider = "BASIC"
    }

    val createTargetTable =
      s"""
         |CREATE TABLE target_table (
         |  `ip` STRING,
         |  `url` STRING,
         |  `times` BIGINT,
         |  window_start TIMESTAMP,
         |  window_end TIMESTAMP
         |)
         |WITH (
         |  'connector' = 'kinesis',
         |  'stream' = 'waf-target',
         |  'aws.credentials.provider' = '${credentialsProvider}',
         |  'aws.credentials.basic.secretkey' = '${params.sk}',
         |  'aws.credentials.basic.accesskeyid' = '${params.ak}',
         |  'aws.region' = '${params.awsRgeion}',
         |  'format' = 'json'
         |);
         |""".stripMargin
    tEnv.executeSql(createTargetTable)

    val mysqlTemporaryTable =
      s"""
        |CREATE TEMPORARY TABLE dim_tb (
        |  id INT,
        |  trigger_value int,
        |  url STRING,
        |  proc_time AS PROCTIME()
        |) WITH (
        |  'connector' = 'jdbc',
        |  'url' = '${params.mysqlHost}',
        |  'table-name' = '${params.mysqlTable}',
        |  'username' = '${params.mysqlUser}',
        |  'password' = '${params.mysqlPassword}',
        |  'lookup.cache.ttl' = '${params.cacheTTL}',
        |  'lookup.cache.max-rows' = '${params.cacheMaxrows}'
        |);
        |""".stripMargin

    tEnv.executeSql(mysqlTemporaryTable)

    val calcSql =
      s"""
         |INSERT INTO target_table
         |select
         |t.ip,
         |t.url,
         |count(1) as times,
         |TUMBLE_START(t.ts, INTERVAL '${params.windowSize}' SECONDS) as window_start,
         |TUMBLE_END(t.ts, INTERVAL '${params.windowSize}' SECONDS) as window_end
         |from (
         |SELECT
         |s.ip as ip ,
         |s.url as url,
         |s.ts as ts ,
         |d.trigger_value as trigger_value,
         |d.url as dim_url
         |FROM source_table as s
         |join dim_tb  FOR SYSTEM_TIME  AS OF  s.ts  AS d
         |on s.url=d.url) as t
         | GROUP BY TUMBLE(t.ts, INTERVAL '${params.windowSize}' SECONDS),t.ip,t.url,t.trigger_value
         | HAVING count(1)>=t.trigger_value;
         |""".stripMargin
    val stat = tEnv.createStatementSet()
    stat.addInsertSql(calcSql)
    stat.execute()

  }

  def createSourceFromStaticConfig(env: StreamExecutionEnvironment, params: ParamsModel.LookupJoinParams): DataStream[String] = {
    val consumerConfig = new Properties()
    consumerConfig.put(AWSConfigConstants.AWS_REGION, params.awsRgeion)
    // 生产环境不用AKSK，本地调试可以使用
    if (params.projectEnv == "local") {
      consumerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, params.ak)
      consumerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, params.sk)

    }
    // 从哪个位置消费
    consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, params.streamInitPosition)
    if (params.streamInitPosition.equalsIgnoreCase(StreamPos.AT_TIMESTAMP.toString)) {
      consumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, params.streamInitialTimestamp)
    }
    val kinesis = env.addSource(new FlinkKinesisConsumer[String](
      params.inputStreamName, new SimpleStringSchema(), consumerConfig))
    kinesis
  }

}
