package com.aws.analytics


import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
import com.aws.analytics.model.ParamsModel
import com.aws.analytics.util.{ParameterToolUtils, StreamPos}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.kinesis.shaded.org.apache.flink.connector.aws.config.AWSConfigConstants
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.{LocalStreamEnvironment, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, FieldExpression, Schema}
import org.apache.flink.types.Row
import org.apache.logging.log4j.LogManager

import java.util.Properties

/**
 * 时态表函数join，可以将维表发送到KDS或者MSK，将append stream转为changelog。
 * 相比Lookup join ,时态表函数能够从流中加载维度数据到state并实时更新，同时避免依赖MySQL等Connector,带来的缓存cache更新问题及
 * 未命中cache造成对后端server的性能压力. 缺点是维表初次从流中加载时，没有方式可以保证维表被加载完毕后才执行join，因此启动时可能会有
 * 关联不到的情况，但这仅存在启动作业时，从维表KDS消费所有数据的时间，这个时间会比较短。
 */
object StreamingTemporaryHOPJoinWAF {
  private val log = LogManager.getLogger(StreamingTemporaryHOPJoinWAF.getClass)

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
    val params = ParameterToolUtils.genHOPTemporaryJoinParams(parameter)
    log.info("waf-hop-temporary-join-params" + params.toString)

    // 创建 table env，流模式
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    val conf = new Configuration()
    conf.setString("pipeline.name", "kda-waf-hop-temporary-join");
    tEnv.getConfig.addConfiguration(conf)

    val source = createSourceFromStaticConfig(env, params)
    val data = source.map(line => {
      try {
        val dataArray = line.replaceAll("\n", "").split("\\t")
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
			   |  'stream' = '${params.targetStreamName}',
			   |  'aws.credentials.provider' = '${credentialsProvider}',
			   |  'aws.credentials.basic.secretkey' = '${params.sk}',
			   |  'aws.credentials.basic.accesskeyid' = '${params.ak}',
			   |  'aws.region' = '${params.awsRgeion}',
			   |  'format' = 'json'
			   |);
			   |""".stripMargin
    tEnv.executeSql(createTargetTable)


    val createDimTable =
      s"""
			   |CREATE TABLE dim_tb (
			   |  `trigger_value` STRING,
			   |  `url` STRING,
			   |   proc_time AS PROCTIME()
			   |)
			   |WITH (
			   |  'connector' = 'kinesis',
			   |  'stream' = '${params.dimStreamName}',
			   |  'aws.credentials.provider' = '${credentialsProvider}',
			   |  'aws.credentials.basic.secretkey' = '${params.sk}',
			   |  'aws.credentials.basic.accesskeyid' = '${params.ak}',
			   |  'aws.region' = '${params.awsRgeion}',
			   |  'scan.stream.initpos' = '${params.dimStreamInitPosition}',
			   |  'format' = 'json'
			   |);
			   |""".stripMargin
    tEnv.executeSql(createDimTable)


    // append stream to changelog stream
    val dim_view =
      """
			  |CREATE VIEW dim_view AS
			  |SELECT url, trigger_value,proc_time
			  |  FROM (
			  |      SELECT *,
			  |      ROW_NUMBER() OVER (PARTITION BY url
			  |         ORDER BY proc_time DESC) AS rowNum
			  |      FROM dim_tb )
			  |WHERE rowNum = 1;
			  |""".stripMargin
    tEnv.executeSql(dim_view)


    val dimTTF = tEnv.sqlQuery("select * from dim_view").createTemporalTableFunction($"proc_time", $"url")
    //,t.trigger_value
    tEnv.createTemporarySystemFunction("dimTTF", dimTTF)
    val calcSql =
      s"""
			   |INSERT INTO target_table
			   |SELECT
			   |t.ip,
			   |t.url,
			   |count(1) as times,
			   |HOP_START(t.ts, INTERVAL '${params.windowInterval}' SECONDS, INTERVAL '${params.windowSize}' SECONDS) as window_start,
			   |HOP_END(t.ts, INTERVAL '${params.windowInterval}' SECONDS, INTERVAL '${params.windowSize}' SECONDS) as window_end
			   |from (
			   |SELECT
			   |s.ip as ip ,
			   |s.url as url,
			   |s.ts as ts ,
			   |d.trigger_value as trigger_value,
			   |d.url as dim_url
			   |FROM source_table as s,
			   |Lateral table (dimTTF(s.ts)) AS d
			   |where s.url=d.url) as t
			   |GROUP BY HOP(t.ts, INTERVAL '${params.windowInterval}' SECONDS, INTERVAL '${params.windowSize}' SECONDS),t.ip,t.url,t.trigger_value
			   | HAVING count(1)>=t.trigger_value;
			   |""".stripMargin

    //tEnv.executeSql(calcSql.replace("INSERT INTO target_table","")).print()
    print(calcSql)
    val stat = tEnv.createStatementSet()
    stat.addInsertSql(calcSql)
    stat.execute()
    //		val calcSql =
    //		s"""
    //		   |SELECT
    //		   |t.ip,
    //		   |t.url,
    //		   |count(1) as times,
    //		   |HOP_START(t.ts, INTERVAL '${params.windowSize}' SECONDS, INTERVAL '${params.windowInterval}' SECONDS) as window_start,
    //		   |HOP_END(t.ts, INTERVAL '${params.windowSize}' SECONDS, INTERVAL '${params.windowInterval}' SECONDS) as window_end
    //		   |from (
    //		   |SELECT
    //		   |s.ip as ip ,
    //		   |s.url as url,
    //		   |s.ts as ts ,
    //		   |d.trigger_value as trigger_value,
    //		   |d.url as dim_url
    //		   |FROM source_table as s,
    //		   |Lateral table (dimTTF(s.ts)) AS d
    //		   |where s.url=d.url) as t
    //		   |GROUP BY HOP(t.ts, INTERVAL '${params.windowSize}' SECONDS, INTERVAL '${params.windowInterval}' SECONDS),t.ip,t.url,t.trigger_value
    //		   | HAVING count(1)>=t.trigger_value;
    //		   |""".stripMargin
    //		val result = tEnv.executeSql(calcSql)
    //		result.print()
  }

  def createSourceFromStaticConfig(env: StreamExecutionEnvironment, params: ParamsModel.HOPTemporaryJoinParams): DataStream[String] = {
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