# SparkLog
基于spark streaming日志设计

架构图：
![](https://github.com/tsmairc/SparkLog/blob/master/image/20170908135049.png?raw=true)

### 原理
原有系统通过rocketmq发送消息，然后spark streaming接收信息并写入到hbase。spark job(基于spring的定时任务),定时跑任务从hbase读取信息分析写到mysql数据库中。所有任务都是跑在yarn上。

### 部署环境
cloudera_manager yarn spark hbase 

### spark streaming代码
spark streaming主要是通过接收器消息mq消息，然后通过spark sql 和 rdd去处理数据，最后写入hbase
* 1.spark streaming接收器
```java
public class JavaCustomReceiver extends Receiver<InterfaceLog>{
  
  private String namesrvAddr;
  private int consumeMessageBatchMaxSize;
  private long pullInterval;
  
  public JavaCustomReceiver(){
    super(StorageLevel.MEMORY_AND_DISK_2());
   //初始化rocketmq启动参数
  }
  
  @Override
  public void onStart(){
    //Start the thread that receives data over a connection
    new Thread(){
      @Override
      public void run(){
        receive();
      }
    }.start();
  }
  
  @Override
  public void onStop(){
    //There is nothing much to do as the thread calling receive()
    //is designed to stop by itself if isStopped() returns false
  }
  
  /**
   * 通过MQ接收数据
   */
  private void receive(){
    String[] namesrvAddrs = namesrvAddr.split(",");
    for(int i = 0; i < namesrvAddrs.length; i++){
      String _namesrvAddr = namesrvAddrs[i];
      if(StringUtils.isBlank(_namesrvAddr)){
        continue;
      }
      
      _namesrvAddr = _namesrvAddr.trim();
      
      
      try{
        LogConsumer(_namesrvAddr);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
  }
  
  //消费日志
  private void LogConsumer(String namesrvAddr) throws Exception{
	//设置mq启动数据
    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(xxx);
    consumer.setNamesrvAddr(xxx);
    consumer.setInstanceName(xxx);
    consumer.setMessageModel(MessageModel.CLUSTERING);//集群模式
    consumer.subscribe(xxx, "*");
    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
    consumer.setConsumeMessageBatchMaxSize(consumeMessageBatchMaxSize);
    consumer.setPullInterval(pullInterval);
    consumer.registerMessageListener(new MessageListenerConcurrently(){
      
      @Override
      public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context){
        for(MessageExt messageExt : msgs){
          byte[] bytes = messageExt.getBody();
          //接收mq信息
        }
        
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
      }
    });
    
    consumer.start();
  }
}
```
* 2.spark streaming写hbase
```java

public class MinSparkStream implements Serializable{
  
  @SuppressWarnings({ "resource" })
  public void start() throws Exception{
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    
    /*
	本地测试代码，无需spark环境启动
    SparkConf sparkConf = new SparkConf().setMaster("local[2]").set("spark.executor.memory", "1g").setAppName("zop-spark");
    JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
    */
    
    JavaStreamingContext jsc = new JavaStreamingContext(SparkUtils.createSparkContext(), Durations.seconds(30));//设置30秒读一次
    
    JavaReceiverInputDStream<Log> lines = jsc.receiverStream(new JavaCustomReceiver());//启动接收器
    //Convert RDDs of the words DStream to DataFrame and run SQL query
    lines.foreachRDD(new VoidFunction2<JavaRDD<Log>, Time>(){
      @Override
      public void call(JavaRDD<Log> rdd, Time time) throws Exception{
        try{
          SparkSession sparkSession = SparkSessionSingleton.getInstance(rdd.context().getConf());
          
          Dataset<Row> logsDataFrame = sparkSession.createDataFrame(rdd, Log.class);
          //Creates a temporary view using the DataFrame
          logsDataFrame.createOrReplaceTempView("log_temp");
		  
	  //下面代码主要封装了spark sql的代码，从上面的spark临时表中获取数据，分析
          MinSparkHandler minSparkHandler = new MinSparkHandler();
          
          //测试维度，处理数据并写入hbase
          minSparkHandler.dataHandler(sparkSession, Consts.APP_TABLE, SparkSqlFactory.getAppSqlFromMq("log_temp"));
          
        }
        catch(Exception e){
          System.out.println(ExceptionUtils.getFullStackTrace(e));
        }
      }
    });
    
    jsc.start();
    jsc.awaitTermination();
  }
  
  public static void main(String[] args) throws Exception{
    MinSparkStream minSparkStream = new MinSparkStream();
    minSparkStream.start();
  }
}

```

* 3.spark sql
```java
public void dataHandler(SparkSession spark, final String table_type, String spark_sql){
  final String batch_id = xxx;
  //启动spark sql分析
  Dataset<Row> logsDataFrame = spark.sql(spark_sql);
  //对spark sql运行后的表的每行分析
  logsDataFrame.foreachPartition(new ForeachPartitionFunction<Row>(){
    
    @Override
    public void call(Iterator<Row> t) throws Exception{
      Map<String, List<Map<String, Object>>> rows = new HashMap<String, List<Map<String, Object>>>();
      while(t.hasNext()){
	//得到表的一行数据
        Row row = t.next();
	//将行数据转换为map
        Map<String, Object> params = rowToMap(row, batch_id, table_type);
	//写入hbase代码非常简单，这里不展示
      }
    }
  }
}
            
```

### spark job
spark job的任务是启动定时任务，以秒分钟天月等维度去分析数据，这里的做法跟spring中定时任务的做法一样。下面代码是spark提供的方法，直接读hbase后生成rdd
```java
//获取hbase连接配置
Configuration conf = HBaseUtils.getConfiguration();
//直接从hbase获取数据
JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = 
new JavaHBaseContext(javaSparkContext, us).hbaseApiRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

//创建临时表
createDataFrame(sqlContext, hbaseRDD, table_month);
//按xx纬度统计
countByConf(sqlContext, table_month);
```

### 分钟维度日志分析
```java
//spark sql
Dataset<Row> countData = sqlContext.sql(spark_sql);
//采用foreachPartition方式，根据task个数进行分布式处理
countData.foreachPartition(new ForeachPartitionFunction<Row>(){
  @Override
  public void call(Iterator<Row> t) throws Exception{
    List<Log> inertParams = new ArrayList<Log>();
    List<Log> delParams = new ArrayList<Log>();
    while(t.hasNext()){
      Row row = t.next();
      String begin_time = row.getString(10);
      if(begin_time == null || begin_time.length() < 6 || !table_month.equals(begin_time.substring(0, 6))){
        continue;
      }
      //将row中的数据组装成列表对象
      installBatchParams(row, inertParams, delParams, count_type);
    }
    //统计数据写到关系型数据库
    if(inertParams.size() > 0){
      dao.operStaticTable(table_name, delParams, inertParams);
    }
  }
});
```
