pentaho-kafka-consumer
======================

Apache Kafka consumer step plug-in for Pentaho Kettle.

[![Build Status](https://travis-ci.org/RuckusWirelessIL/pentaho-kafka-consumer.png)](https://travis-ci.org/RuckusWirelessIL/pentaho-kafka-consumer)


### Screenshots ###

![Using Apache Kafka Consumer in Kettle](https://raw.github.com/RuckusWirelessIL/pentaho-kafka-consumer/master/doc/example.png)


### Apache Kafka Compatibility ###

The consumer depends on Apache Kafka 0.8.1.1, which means that the broker must be of 0.8.x version or later.

### Maximum Duration Of Consumption ###

Note that the maximum duration of consumption is a limit on the duration of the
entire step, *not* an individual read. This means that if you have a maximum
duration of 5000ms, your transformation will stop after 5s, whether or
not more data exists and independent of how fast each message is fetched from
the topic. If you want to stop reading messages when the topic has no more
messages, see the section on _Empty topic handling_.

### Empty topic handling ###

If you want the step to halt when there are no more messages available on the
topic, check the "Stop on empty topic" checkbox in the configuration dialog. The
default timeout to wait for messages is 1000ms, but you can override this by
setting the "consumer.timeout.ms" property in the dialog. If you configure a
timeout without checking the box, an empty topic will be considered a failure
case.

### Installation ###

1. Download ```pentaho-kafka-consumer``` Zip archive from [latest release page](https://github.com/RuckusWirelessIL/pentaho-kafka-consumer/releases/latest).
2. Extract downloaded archive into *plugins/steps* directory of your Pentaho Data Integration distribution.


### Building from source code ###

```
mvn clean package
```

### 华为kerberos认证

使用步骤：

1. kettle6.0安装目录，创建conf文件夹

   ```
   ├── krb5.conf
   ├── user.jaas.conf
   └── user.keytab
   ```

2. Spoon.bat或者Spoon.sh修改OPT变量增加以下部分，地址修改为自己位置
   ```
   OPT="$OPT -Djava.security.auth.login.config=/Users/xiao/dev/kettle/conf/user.jaas.conf -Djava.security.krb5.conf=/Users/xiao/dev/kettle/conf/krb5.conf"
   ```

3. user.jaas.conf文件内容如下，按照实际进行修改

   ```
   EsClient{
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   keyTab="/etc/user.keytab"
   principal="chinaoly"
   useTicketCache=false
   storeKey=true
   debug=true;
   };
   Client{
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   keyTab="/etc/user.keytab"
   principal="chinaoly"
   useTicketCache=false
   storeKey=true
   debug=true;
   };
   StormClient{
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   keyTab="/etc/user.keytab"
   principal="chinaoly"
   useTicketCache=false
   storeKey=true
   debug=true;
   };
   KafkaClient{
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   keyTab="/etc/user.keytab"
   principal="chinaoly"
   useTicketCache=false
   storeKey=true
   debug=true;
   };
   ```

4. 使用kerberos认证必须设置`isSecureMode` 为`true`，除了`bootstrap.servers`以外其他默认即可。
