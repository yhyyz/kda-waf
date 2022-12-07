## KDS->Flink->KDS
* KDA Flink(VERSION=1.15)
* 解析WAF日志，根据定义规则实时触发告警
#### 本地调试参数,KDA Console参数与之相同，去掉参数前的-即可
* Streaming WAF
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
* Streaming Lookup Join
```shell
-project_env local
-aws_region us-east-1
-ak xxx
-sk xxx
-input_stream_name waf-source
-stream_init_position LATEST
-target_stream_name waf-target
-window_size 10
-mysql_host jdbc:mysql://xxxx:3306/test_db?useSSL=false
-mysql_user xxx
-mysql_password xxxx
-mysql_table dim_tb
-cache_ttl 3600s
-cache_maxrows 1000
```
```sql
CREATE DATABASE test_db;
CREATE TABLE `dim_tb` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `trigger_value` int(11) NOT NULL,
  `url` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8

insert into dim_tb(trigger_value,url) values(2,'a.html');
insert into dim_tb(trigger_value,url) values(3,'b.html');
```

* Streaming Temporary Join
```shell
-project_env local
-aws_region us-east-1
-ak xxx
-sk xxx
-input_stream_name waf-source
-stream_init_position LATEST
-target_stream_name waf-target
-window_size 10
-dim_stream_name waf-dim
-dim_stream_init_position TRIM_HORIZON
```
```shell
# {"url":"a.html","trigger_value":2}
aws --profile ue1 --region us-east-1 kinesis put-record \
    --stream-name waf-dim \
    --data "eyJ1cmwiOiJhLmh0bWwiLCJ0cmlnZ2VyX3ZhbHVlIjoyfQo=" \
    --partition-key 1  
```


* Streaming Temporary HOP Join 
```shell
-project_env local
-aws_region us-east-1
-ak xxx
-sk xxx
-input_stream_name waf-source
-stream_init_position LATEST
-target_stream_name waf-target
-window_size 10 # 10s的窗口
-window_interval 5  # 5s滑动一次
-dim_stream_name waf-dim
-dim_stream_init_position TRIM_HORIZON
```
```shell
# {"url":"a.html","trigger_value":2}
aws --profile ue1 --region us-east-1 kinesis put-record \
    --stream-name waf-dim \
    --data "eyJ1cmwiOiJhLmh0bWwiLCJ0cmlnZ2VyX3ZhbHVlIjoyfQo=" \
    --partition-key 1  
```


#### build
```sh
mvn clean package -Dscope.type=provided
# 使用不同类时，注意修改pom的MainClass
```

![](https://pcmyp.oss-accelerate.aliyuncs.com/markdown/20221203022004.png)
![](https://pcmyp.oss-accelerate.aliyuncs.com/markdown/20221203022134.png)
* Lookup Join
![](https://pcmyp.oss-accelerate.aliyuncs.com/markdown/20221205133716.png)
* Temporary Join  
![](https://pcmyp.oss-accelerate.aliyuncs.com/markdown/20221205133831.png)