## KDS->Flink->KDS
* KDA Flink(VERSION=1.15)
* 解析WAF日志，根据定义规则实时触发告警
#### 本地调试参数,KDA Console参数与之相同，去掉参数前的-即可
```shell
-project_env local or prod # local表示本地运行，prod表示KDA运行
-aws_region us-east-1 #KDS REGION
-ak xxx # project_env=local时需要
-sk xxx # project_env=local时需要
-input_stream_name xxx # 读取KDS NAME
-stream_init_position # LATEST or TRIM_HORIZON or AT_TIMESTAMP
-target_stream_name xxx # 写入的KDS NAME
-window_size 10 # 滚动窗口的间隔，单位是秒，10表示10秒
-trigger_value 50 # 当次数大于等于该值时发送到Target KDS
```
#### build
```sh
mvn clean package -Dscope.type=provided
```