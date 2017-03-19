package com.don.session.spark;

import com.don.session.conf.ConfigurationManager;
import com.don.session.constant.Constants;
import com.don.session.dao.ISessionAggrStatDao;
import com.don.session.dao.ISessionDetailDao;
import com.don.session.dao.ISessionRandomExtractDao;
import com.don.session.dao.ITaskDao;
import com.don.session.dao.impl.DaoFactory;
import com.don.session.domain.SessionAggrStat;
import com.don.session.domain.SessionDetail;
import com.don.session.domain.SessionRandomExtract;
import com.don.session.domain.Task;
import com.don.session.util.*;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.json.JSONException;
import org.json.JSONObject;

import scala.Tuple2;

import java.util.*;

/**
 * 用户访问session分析Spark作业
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * 1、时间范围：起始日期 ~ 结束日期
 * 2、性别：男或女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个Session中的任何一个Action搜索过指定的关键词，那么Session就符合条件
 * 7、点击品类：多个品类，只要某个Session中的任何一个Action点击过某个品类，那么Session就符合条件
 *
 * 我们的Spark作业如何接收用户创建的任务呢？
 *
 * J2EE平台在接收用户创建任务的请求之后会将任务信息插入MYSQL的Task表中，任务参数以JSON格式封闭在Task_param字段中
 *
 * 接着J2EE平台在执行我们的Spark-submit Shell脚本，并将Taskid作为参数传递给Spark-submit Shell脚本
 * Spark-submit Shell脚本在执行时，是可以接收参数的，并且会将接收的参数传递给Spark作业的Main函数
 * 参数就封闭在Main函数的args数组中
 *
 * 这是Spark本身提供的特性
 *
 * Created by Cao Wei Dong on 2017/2/12.
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
//        args = new String[]{"1"};
        //构建Spark上下文环境
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local")
                .set("spark.ui.port","9785");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = getSQLContext(sc.sc());

        //生成模拟测试数据
        mockData(sc, sqlContext);
        //创建需要用使用的DAO组件
        ITaskDao taskDao = DaoFactory.getTaskDAO();

        //进行Session粒度的数据聚合
        //从User_visit_action表中查询出来指定日期范围内的行为数据

        //首先查询出来指定的任务
        long taskId = ParamUtils.getLongFromArgs(args);

        Task task = taskDao.findById(taskId);

        JSONObject taskParam = null;
        try {
            taskParam = new JSONObject(task.getTaskParam());
        } catch (JSONException e) {
            e.printStackTrace();
        }

        //如果要进行Session粒度的数据聚合
        //要从User_visit_action表中查询出来指定日期范围内的行为数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);

        JavaPairRDD<String, Row> sessionId2actionRDD = getsessionId2ActionRDD(actionRDD);
        //首先可以将数据按照Session_id进行GroupByKey分组
        //此时的数据的粒度就是session粒度了，然后将sesion粒度的数据与用户信息数据
        //进行join
        //然后就可以获取到Session粒度的数据，同时数据里面还包含了Sesison对应的User的信息
        JavaPairRDD<String, String> sessionId2AggregateInfoRDD = aggregateegateBySession(sqlContext, actionRDD);

        //接着，就要针对Session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
        //相当于我们自己编写的牌子，是要访问外面的任务参数对象的
        //匿名内部类（牌子函数）访问外部对象是要给外部对象使用final修饰的
        //重构，同时进行过滤和统计
        Accumulator<String> sessionAggregateStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());

        JavaPairRDD<String, String> filteredSessionId2AggregateInfoRDD =
                filterSessionAndAggregateStat(sessionId2AggregateInfoRDD, taskParam, sessionAggregateStatAccumulator);


        randomExtractSession(task.getTaskid(), filteredSessionId2AggregateInfoRDD, sessionId2actionRDD);


        //计算出各个范围的Session占比，并写入Mysql数据库
        calculateAndPersistAggregateStat(sessionAggregateStatAccumulator.value(), task.getTaskid());
        /*
         * Session聚合统计（统计出访问时长和访问步长，各个敬意的Session数量占总Session数量的比例）
         *
         * 重构实现思路：
         * 1、不要去生成任何新的RDD
         * 2、不要去单独遍历一刻Session的数据
         * 3、可以在进行Session聚合的时候就直接计算出来每个Session的访问时长和访问步长
         * 4、在进行过滤的时候本来就要遍历所有的聚合Session信息，此时就可以在某个Session通过筛选条件后
         *  将其访问时长和访问步长累加到自定义Accumulator上面去
         *
         */

        getTop10Category(filteredSessionId2AggregateInfoRDD, sessionId2actionRDD);



        //关闭Spark上下文
        sc.close();
    }



    /**
     * 获取SQLContext
     * 如果是在本地测试环境的话，那么生成SQLContext对象
     * 如果是在生产环境运行的话，那么就生成HiveContext对象
     *
     * @param sc SparkContext
     * @return sqlContext & HiveContext
     */
    private static SQLContext getSQLContext(SparkContext sc) {

        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }



    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc, sqlContext);
        }
    }

    /**
     * 获取指定日期范围内的用户访问行为数据
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql = "select * " +
                "from user_visit_action " +
                "where date>='" + startDate + "' " +
                "and date<='" + endDate + "'";

        DataFrame actionDF = sqlContext.sql(sql);

        return actionDF.javaRDD();
    }

    /**
     * 对行为数据按session粒度进行聚合
     * @param sqlContext Spark上下文
     * @param actionRDD 行为数据RDD
     * @return Session聚合结果
     */
    private static JavaPairRDD<String, String> aggregateegateBySession(SQLContext sqlContext, final JavaRDD<Row> actionRDD) {
        //ActionRDD中的元素是Row，一个Row就是一行用户访问行为记录，比如一次点击或者搜索
        //将Row映射成<sessionId,Row>的格式
        //Session数据格式：date, user_id, session_id, page_id, action_time, search_keyword, click_category_id click_product_id, order_category_ids, order_product_ids, pay_category_ids, pay_product_ids, city_id
        JavaPairRDD<String, Row> sessionId2ActionRDD = actionRDD.mapToPair(
                new PairFunction<Row, String, Row>() {
                    /**
                     * PairFunction
                     * 第一个参数相当于是函数的输入
                     * 第二个参数和第三个参数相当于是函数的输出Tuple)，分别是Tuple第一个和第二个值
                     */
                    @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        return new Tuple2<>(row.getString(2), row);
                    }

                }
        );

        //对行为数据按Session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD = sessionId2ActionRDD.groupByKey();

        //对每一个Session分组进行聚合，将Session中所有的搜索词和点击品类都聚合起来
        //到此为止，获取的数据格式如下 ：<userid, partAggregateInfo(sessionId, searchKeywords, clickCategoryIds)>
        JavaPairRDD<Long, String> userid2PartAggregateInfoRDD = sessionId2ActionsRDD.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {

                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        String sessionId = tuple._1;
                        Iterator<Row> iter = tuple._2.iterator();

                        StringBuilder searchKeywordsBuffer = new StringBuilder("");
                        StringBuilder clickCategoryIdsBuffer = new StringBuilder("");

                        Long userId = null;

                        //Session的起始和结束时间
                        Date startTime = null;
                        Date endTime = null;

                        //Session的访问步长
                        int stepLength = 0;

                        //遍历Session所有的访问行为
                        while (iter.hasNext()) {
                            //提取每个访问行为的搜索词字段和点击品类字段
                            Row row = iter.next();
                            if (userId == null) {
                                userId = row.getLong(1);
                            }
                            String searchKeyword = row.getString(5);
                            Object clickCategoryId = row.get(6);

                            //并不是每一行访问行为都有SearchKeyword和ClickCategoryId两个字段的
                            //其实，只有搜索行为是有SearchKeyword字段的
                            //只有点击品类的行为，是有ClickCategoryId字段的
                            //所以，任何一行行为数据，都不可能两个字段都有，所有数据是可能出现null值的

                            //我们6决定是否将搜索词或者点击品类ID拼接到字符串中去
                            //首先要满足：不能是null值、
                            //其次，之前的字符串中还没有搜索词或者点击品类id

                            if (StringUtils.isNotEmpty(searchKeyword)) {
                                if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                                    searchKeywordsBuffer.append(searchKeyword);
                                    searchKeywordsBuffer.append(",");
                                }
                            }

                            if (clickCategoryId != null) {
                                if (!clickCategoryIdsBuffer.toString().contains(
                                        String.valueOf(clickCategoryId)
                                )) {
                                    clickCategoryIdsBuffer.append(clickCategoryId);
                                    clickCategoryIdsBuffer.append(",");
                                }
                            }

                            //计算Session开始和结束时间
                            Date actionTime = DateUtils.parseTime(row.getString(4));

                            if (null == startTime) {
                                startTime = actionTime;
                            }
                            if (null == endTime) {
                                endTime = actionTime;
                            }

                            assert startTime != null;
                            assert actionTime != null;
                            if (actionTime.before(startTime)) {
                                startTime = actionTime;
                            }
                            if (actionTime.after(endTime)) {
                                endTime = actionTime;
                            }

                            //计算Session访问步长
                            stepLength++;
                        }

                        String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                        //计算Session访问时长（秒）
                        assert endTime != null;
                        long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

                        //我们返回的数据格式，即<sessionId, partAggregateInfo>
                        //但是，这一步聚合完了以后，其实，我们是还需要将每一行数据跟对应的用户信息进行进行聚合
                        //问题就来了，如果是跟用户信息进行聚合的话，那么key就不应该是sessionId
                        //就应该是userid，才能够跟<userid, Row>格式的用户信息进行聚合
                        //如果我们这里直接返回<session, partAggregateInfo>, 还得再做一次MapToPair算子
                        //将RDD映射成<userid, parAggregateInfo>的格式，那么就多此一举

                        //所以，这里其实可以直接返回的数据格式就是<userid, partAggregateInfo>
                        //然后跟用户信息join的时候，将partAggregateInfo关联上userInfo
                        //然后南直接将返回的Tuple的Key设置成sessionId
                        //然后再直接将返回的Tuple的Key设置成sessionId
                        //最后的数据格式还是<sessionId, fullAggregateInfo>

                        //聚合数据用什么样的格式进行拼接？
                        //我们这里统一定义，使用key=value|key=value
                        String partAggregateInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                                + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                                + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                                + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                                + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                                + Constants.FIELD_START_TIME + "=" + DateUtils.getFormatTime(startTime);

                        return new Tuple2<>(userId, partAggregateInfo);
                    }
                }
        );

        //查询所有用户数据,并映射成<userid, Row> 的格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();

        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(
                new PairFunction<Row, Long, Row>() {
                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        return new Tuple2<>(row.getLong(0), row);
                    }
                }
        );

        //将session粒度聚合数据，与用户信息进行join
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggregateInfoRDD.join(userid2InfoRDD);

        //对join起来的数据进行拼接，并且返回<sessionId, fullAggregateInfo> 格式的数据
        return userid2FullInfoRDD.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                        String partAggregateInfo = tuple._2._1;
                        Row userInfoRow = tuple._2._2;

                        String sessionId = StringUtils.getValueFromStringByName(partAggregateInfo, "\\|", Constants.FIELD_SESSION_ID);

                        int age = userInfoRow.getInt(3);

                        //用户职业
                        String professional = userInfoRow.getString(4);
                        //用户所在城市
                        String city = userInfoRow.getString(5);
                        //用户性别
                        String sex = userInfoRow.getString(6);

                        //
                        String fullAggregateInfo = partAggregateInfo + "|"
                                + Constants.FIELD_AGE + "=" + age + "|"
                                + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                                + Constants.FIELD_CITY + "=" + city + "|"
                                + Constants.FIELD_SEX + "=" + sex;
                        return new Tuple2<>(sessionId, fullAggregateInfo);
                    }
                }
        );
    }

    /**
     * 过滤Session数据，并进行聚合统计
     * @param sessionId2AggregateInfoRDD Session信息
     * @param taskParam task条件
     * @return 过滤结果
     */
    private static JavaPairRDD<String,String> filterSessionAndAggregateStat(
            JavaPairRDD<String, String> sessionId2AggregateInfoRDD,
            final JSONObject taskParam,
            final Accumulator<String> sessionAggregateStatAccumulator) {
        //为了使用我们后面的ValieUtils，首先将所有的筛选参数拼接成一个连接串
        //此外这里其实大家不要觉得是多此一举
        //其实我们是给后面的性能优化埋下一个伏笔的

        //任务参数：开始年龄
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        //任务参数：结束年龄
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        //任务参数：职业
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        //任务参数：城市
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        //任务参数：性别
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        //任务参数：关键词
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        //任务参数：品类
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        //参数拼接
        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");

        if (_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;

        //根据筛选参数进行过滤
        return sessionId2AggregateInfoRDD.filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        //首先从tuple中获取聚合数据
                        String aggregateInfo = tuple._2();

                        //接着，合资按照筛选条件进行过滤
                        //按照年龄范围进行过滤（startAge， endAge）
                        if (!ValidUtils.validBetween(aggregateInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }

                        //按照职业范围进行过滤(professionals)
                        //互联网、IT、软件
                        if (!ValidUtils.validIn(aggregateInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }

                        //按照老友记范围进行过滤(cities)
                        //北京， 上海， 广州， 深圳
                        //成都
                        if (!ValidUtils.validEqual(aggregateInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        //按照点击品类id进行过滤
                        if (!ValidUtils.validIn(aggregateInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        //按照搜索词进行过滤
                        //Session可能搜索了，火锅、蛋糕、等
                        //筛选条件可能是火锅、串串香、iphone
                        //那么，in这个校验方法主要判定Session搜索的词中有任何一个与筛选条件中任何一个搜索词相当，即通过
                        if (!ValidUtils.validIn(aggregateInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        //按照点击品类id进行过滤
                        if (!ValidUtils.validIn(aggregateInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }

                        //如果经过了之前的多个过滤条件之后，程序能够直到这里
                        //那么就说明该Session是通过了用户指定的筛选条件的，也就是需要保留的Session
                        //那么就要对Session的访问时长和访问步长进行统计，根据Session对应的范围进行相应的累加计数

                        //主要直到这一步，那么就是需要计数的Session
                        sessionAggregateStatAccumulator.add(Constants.SESSION_COUNT);

                        //计算出Session的访问时长和访问步长的范围，并进行相应折累加
                        //访问时长
                        long visitLength = Long.valueOf(getValueByKey(aggregateInfo, Constants.FIELD_VISIT_LENGTH));

                        //访问步长
                        long stepLength = Long.valueOf(getValueByKey(aggregateInfo, Constants.FIELD_STEP_LENGTH));

                        calculateVisitLength(visitLength);
                        calculateStemLength(stepLength);

                        return true;
                    }

                    /**
                     * 计算访问时长范围
                     * @param visitLength 访问步长
                     */
                    private void calculateVisitLength(long visitLength) {
                        if (visitLength >= 1 && visitLength <= 3) {
                            sessionAggregateStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                        } else if (visitLength >= 4 && visitLength <= 6) {
                            sessionAggregateStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        } else if (visitLength >= 7 && visitLength <= 9) {
                            sessionAggregateStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        } else if (visitLength >= 10 && visitLength <= 30) {
                            sessionAggregateStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                        } else if (visitLength > 30 && visitLength <= 60) {
                            sessionAggregateStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        } else if (visitLength > 60 && visitLength <= 180) {
                            sessionAggregateStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                        } else if (visitLength > 600 && visitLength <= 1800) {
                            sessionAggregateStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        } else if (visitLength > 1800) {
                            sessionAggregateStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }
                    }

                    /**
                     * 计算访问步长范围
                     */
                    private void calculateStemLength(long stepLength) {
                        if (stepLength >= 1 && stepLength <= 3) {
                            sessionAggregateStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        } else if (stepLength >= 4 && stepLength <= 6) {
                            sessionAggregateStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        } else if (stepLength >= 7 && stepLength <= 9) {
                            sessionAggregateStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        } else if (stepLength >= 10 && stepLength <= 30) {
                            sessionAggregateStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        } else if (stepLength > 30 && stepLength <= 60) {
                            sessionAggregateStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        } else if (stepLength > 60) {
                            sessionAggregateStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }
                    }
                }
        );
    }



    /**
     * 随机抽取Session
     * @param sessionId2AggregateInfoRDD RDD: <sessionId, SessionInfo>
     * @param sessionId2actionRDD RDD: <sessionId, actionRDD>
     */
    private static void randomExtractSession(
            final long taskId,
            JavaPairRDD<String, String> sessionId2AggregateInfoRDD,
            JavaPairRDD<String, Row> sessionId2actionRDD) {
        //第一步：计算出每天每小时的Session数量，获取<yyyy-MM-dd_HH,sessionId>格式的RDD
        JavaPairRDD<String, String> time2sessionIdRDD = sessionId2AggregateInfoRDD.mapToPair(
                new PairFunction<Tuple2<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
                        String aggregateInfo = tuple._2();
                        String startTime = StringUtils.getValueFromStringByName(
                                aggregateInfo,
                                "\\|",
                                Constants.FIELD_START_TIME);
                        String dateHour = DateUtils.getDateHour(startTime);

                        return new Tuple2<>(dateHour, aggregateInfo);
                    }

                }
        );

        //每天每小时的Session，然后计算出每天每小时的Session抽取索引，遍历每天每小时Session
        //抽取出的Session的聚合数据，写入session_random_extract表
        //所以第一个RDD的Value，应该是Session聚合数据
        Map<String, Object> countMap = time2sessionIdRDD.countByKey();

        //第二步：使用按时间比例随机抽取算法，计算出每天每小时要抽取Session的索引

        //将<yyyy-MM-dd_HH, count>格式的Map转换成<yyyy-MM-dd,<HH,count>>的格式
        final Map<String, Map<String, Long>> dayHourCountMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : countMap.entrySet() ) {
            //获取Map的key：天加小时yyyy-MM-dd_HH
            String dateHour = entry.getKey();
            //获取Map的Value：获取当天指定小时段内的数据时不时
            long count = Long.valueOf(String.valueOf(entry.getValue()));

            //获取天
            String date = dateHour.split("_")[0];
            //获取小时
            String hour = dateHour.split("_")[1];

            //当天小时区间的Key-value对
            Map<String, Long> hourCountMap = dayHourCountMap.get(date);
            //如果输出的<date,<hour, hourCount>>中hour不存在则重新创建
            if (hourCountMap == null) {
                hourCountMap = new HashMap<>();
                dayHourCountMap.put(date, hourCountMap);
            }

            hourCountMap.put(hour, count);
        }

        //开始实现按时间比例随机抽取算法
        //总共要抽取100个Session，先按照天数进行平分
        long extractNumberPerDay = 100 / dayHourCountMap.size();

        //<date, <hour,(index1, index2, index3, index4)>>
        final Map<String, Map<String, List<Integer>>> dateHourExtractMap =
                new HashMap<>();

        //生成<date,<hour, hourCount>>的输出索引
        for (Map.Entry<String, Map<String, Long>> entry :
                dayHourCountMap.entrySet()) {
            String date = entry.getKey();
            Map<String, Long> hourCountMap = entry.getValue();
            
            //计算出这一天的Session总数
            long sessionCount = 0L;
            for (long hourCount :
                    hourCountMap.values()) {
                sessionCount += hourCount;
            }

            //先获取当天每小时的存放随机数的List
            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<>();
                dateHourExtractMap.put(date, hourExtractMap);
            }
            Random random = new Random();

            //遍历每个小时
            for (Map.Entry<String, Long> hourCountEntry :
                    hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();

                //计算每个小时Session数量占据当天总Session数量的比例
                int hourExtractNumber = (int) (((double) count / sessionCount) * extractNumberPerDay);
                if (hourExtractNumber > count) {
                    hourExtractNumber = (int) count;
                }

                //获取当前小时的存放随机数的List
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if (extractIndexList == null) {
                    extractIndexList = new ArrayList<>();
                    hourExtractMap.put(hour, extractIndexList);
                }

                //生成上面计算出来的数量的随机数
                for (int i = 0; i < hourExtractNumber; i++) {
                    //获取随机生成的索引
                    int extractIndex = random.nextInt((int) count);
                    //如果索引号已经抽取，则重新随机生成
                    while (extractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt((int) count);
                    }
                    extractIndexList.add(extractIndex);
                }
            }
        }

        //将每天每的Session union，然后根据随机索引进行抽取
        //执行groupByKey算子，得到<dateHour,(session, aggregateInfo)>
        JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionIdRDD.groupByKey();

        //遍历每天每小时的Session
        //如果发现某个Session恰巧在我们指定的这天这小时的随机抽取索引上
        //那么抽取该Session，直接写入Mysql的random_extract_session表
        //将抽取出来的Session ID返回回来，形成一个新的 JavaRDD<String>
        //最后用抽取出来的sessionId去join它们的访问行为明细数据，写入Session表
        JavaPairRDD<String, String> extractSessionIdsRDD = time2sessionsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
                    @Override
                    public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                        List<Tuple2<String, String>> extractSessionIds = new ArrayList<>();

                        String dateHour = tuple._1();
                        String date = dateHour.split("_")[0];
                        String hour = dateHour.split("_")[1];
                        Iterator<String> iterator = tuple._2().iterator();

                        List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);

                        ISessionRandomExtractDao sessionRandomExtractDao = DaoFactory.getSessionRandomExtractDao();
                        int index = 0;
                        while (iterator.hasNext()) {
                            String sessionAggregateInfo = iterator.next();

                            if (extractIndexList.contains(index)) {
                                String sessionId = StringUtils.getValueFromStringByName(
                                        sessionAggregateInfo,
                                        "\\|",
                                        Constants.FIELD_SESSION_ID
                                );

                                //将数据写入Mysql数据
                                SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                                sessionRandomExtract.setTaskId(taskId);
                                sessionRandomExtract.setSessionId(sessionId);
                                sessionRandomExtract.setStartTime(
                                        StringUtils.getValueFromStringByName(
                                                sessionAggregateInfo,
                                                "\\|",
                                                Constants.FIELD_START_TIME
                                        )
                                );
                                sessionRandomExtract.setSearchKeywords(
                                        StringUtils.getValueFromStringByName(
                                                sessionAggregateInfo,
                                                "\\|",
                                                Constants.FIELD_SEARCH_KEYWORDS
                                        )
                                );
                                sessionRandomExtract.setClickCategoryIds(
                                        StringUtils.getValueFromStringByName(
                                                sessionAggregateInfo,
                                                "\\|",
                                                Constants.FIELD_CLICK_CATEGORY_IDS
                                        )
                                );

                                sessionRandomExtractDao.insert(sessionRandomExtract);

                                //将sessionId加入list
                                extractSessionIds.add(new Tuple2<>(sessionId, sessionId));

                            }
                            index++;
                        }
                        return extractSessionIds;
                    }
                }
        );

        /*
         * 第四步：获取抽取出来的Session的明细数据
         */
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD =
                extractSessionIdsRDD.join(sessionId2actionRDD);
        extractSessionDetailRDD.foreach(
                new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
                    @Override
                    public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                        Row row = tuple._2()._2();

                        SessionDetail sessionDetail = new SessionDetail();
                        sessionDetail.setTaskId(taskId);
                        sessionDetail.setUserId(row.getLong(1));
                        sessionDetail.setSessionId(row.getString(2));
                        sessionDetail.setPageId(row.getLong(3));
                        sessionDetail.setActionTime(row.getString(4));
                        sessionDetail.setSearchKeyWord(row.getString(5));

                        Object clickCategoryId = row.get(6);
                        if (clickCategoryId == null) {
                            sessionDetail.setClickCategoryId(null);
                        }
                        else {
                            sessionDetail.setClickCategoryId(row.getLong(6));
                        }

                        Object clickProductId = row.get(7);
                        if (clickProductId == null)
                            sessionDetail.setClickProductId(null);
                        else
                            sessionDetail.setClickProductId(row.getLong(7));

                        sessionDetail.setOrderCategoryIds(row.getString(8));
                        sessionDetail.setOrderProductIds(row.getString(9));
                        sessionDetail.setPayCategoryIds(row.getString(10));
                        sessionDetail.setPayProductIds(row.getString(11));

                        ISessionDetailDao sessionDetailDao = DaoFactory.
                                getSessionDetailDao();

                        sessionDetailDao.insert(sessionDetail);
                    }
                }
        );
    }

    /**
     * 将JavaRDD转为JavaPairRDD
     * @param actionRDD action rdd
     * @return JavaPairRDD
     */
    private static JavaPairRDD<String,Row> getsessionId2ActionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(
                new PairFunction<Row, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        return new Tuple2<>(row.getString(2), row);
                    }
                }
        );

    }
    /**
     * 计算各Session范围占比，并写入MYSQL
     * @param value Session信息
     * @param taskId task主键
     */
    private static void calculateAndPersistAggregateStat(String value, long taskId) {
        //从Accumulator统计串中获取值
        long session_count = Long.valueOf(getValueByKey(value, Constants.SESSION_COUNT));
        long visit_length_1s_3s = Long.valueOf(getValueByKey(value, Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(getValueByKey(value, Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(getValueByKey(value, Constants.TIME_PERIOD_7s_9s));

        long visit_length_10s_30s = Long.valueOf(getValueByKey(value, Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(getValueByKey(value, Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(getValueByKey(value, Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(getValueByKey(value, Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(getValueByKey(value, Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(getValueByKey(value, Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(getValueByKey(value, Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(getValueByKey(value, Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(getValueByKey(value, Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(getValueByKey(value, Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(getValueByKey(value, Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(getValueByKey(value, Constants.STEP_PERIOD_60));

        //计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.getFormatDouble((double) visit_length_1s_3s / (double) session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.getFormatDouble((double) visit_length_4s_6s / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.getFormatDouble((double) visit_length_7s_9s / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.getFormatDouble((double) visit_length_10s_30s / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.getFormatDouble((double) visit_length_30s_60s / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.getFormatDouble((double) visit_length_1m_3m / (double) session_count, 2);
        double visit_length_3m_10m_ratio =  NumberUtils.getFormatDouble((double) visit_length_3m_10m / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.getFormatDouble((double) visit_length_10m_30m / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.getFormatDouble((double) visit_length_30m / (double) session_count, 2);

        double step_length_1_3_ratio = NumberUtils.getFormatDouble(step_length_1_3 /session_count, 2);
        double step_length_4_6_ratio = NumberUtils.getFormatDouble(step_length_4_6 /session_count, 2);
        double step_length_7_9_ratio = NumberUtils.getFormatDouble(step_length_7_9 /session_count, 2);
        double step_length_10_30_ratio = NumberUtils.getFormatDouble(step_length_10_30 /session_count, 2);
        double step_length_30_60_ratio = NumberUtils.getFormatDouble(step_length_30_60 /session_count, 2);
        double step_length_60_ratio = NumberUtils.getFormatDouble(step_length_60 /session_count, 2);

        //将统计结果封闭为Domain对象
        SessionAggrStat sessionAggregateStat = new SessionAggrStat();

        sessionAggregateStat.setTaskid(taskId);
        sessionAggregateStat.setSession_count(session_count);
        sessionAggregateStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggregateStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggregateStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggregateStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggregateStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggregateStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggregateStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggregateStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggregateStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggregateStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggregateStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggregateStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggregateStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggregateStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggregateStat.setStep_length_60_ratio(step_length_60_ratio);

        //调用DAO插入MYSQL统计结果
        ISessionAggrStatDao sessionAggregateStatDao = DaoFactory.getSessionAggrStatDao();
        sessionAggregateStatDao.insert(sessionAggregateStat);
    }

    private static String getValueByKey(String info, String key) {
        String temp = StringUtils.getValueFromStringByName(info, "\\|", key);
        return (temp == null) ? "-1" : temp;

    }

    private static void getTop10Category(
            JavaPairRDD<String, String> filteredSessionId2AggregateInfoRDD,
            JavaPairRDD<String, Row> sessionId2actionRDD) {
        /*
         * 第一步：获取符合条件的Session访问过的所有品类
         */
        //获取符合条件的Session的访问明细
        JavaPairRDD<String, Row> sessionId2DetailRDD = filteredSessionId2AggregateInfoRDD
                .join(sessionId2actionRDD)
                .mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                        return new Tuple2<>(tuple._1(), tuple._2()._2());

                    }
                });

        //获取Session访问过的所有品类ID
        //访问过指的是：点击过，下单过、支付过的品类
        JavaPairRDD<Long, Long> categoryIdRDD = sessionId2DetailRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2();

                        List<Tuple2<Long, Long>> list = new ArrayList<>();

                        Long clickCategoryId = row.getLong(6);
                        if (null != clickCategoryId) {
                            list.add(new Tuple2<>(clickCategoryId, clickCategoryId));
                        }

                        String orderCategoryIds = row.getString(8);
                        if (orderCategoryIds != null) {
                            String[] orderCategoryIdsSplit = orderCategoryIds.split(",");

                            for (String orderCategoryId :
                                    orderCategoryIdsSplit) {
                                list.add(new Tuple2<>(Long.valueOf(orderCategoryId), Long.valueOf(orderCategoryId)));
                            }
                        }

                        String payCategoryIds = row.getString(10);
                        if (payCategoryIds != null) {
                            String[] payCategoryIdsSplit = payCategoryIds.split(",");
                            for (String payCategoryId :
                                    payCategoryIdsSplit) {
                                list.add(new Tuple2<>(
                                        Long.valueOf(payCategoryId),
                                        Long.valueOf(payCategoryId)
                                ));
                            }
                        }

                        return list;
                    }
                }
        );

        /*
         * 第二步：计算各品类的点击、下单和支付次数
         * 访问明细中，其中三种访问行为是：点击、下单和支付
         * 分别计算各品类点击、下单和支付的次数，可以先对访问明细数据进行过滤
         * 分别过滤出点击、下单和支付行为、然后通过map/reduceByKey等牌子来进行计算
         */
        //计算各个品类的点击次数
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionId2DetailRDD);

        //计算各个品类的下单次数
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionId2DetailRDD);

        //计算各个品类的支付次数
        JavaPairRDD<Long, Long> payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionId2DetailRDD);
    }

    private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2DetailRDD) {
        JavaPairRDD<String, Row> clickActionRDD = sessionId2DetailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {

                        Row row = tuple._2();

                        return Long.valueOf(row.getLong(6)) != null;
                    }
                }
        );

        JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(
                new PairFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
                        Long clickCategoryId = tuple._2().getLong(6);
                        return new Tuple2<>(clickCategoryId, 1L);
                    }
                }
        );

        return clickCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );
    }

    private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2DetailRDD) {
        JavaPairRDD<String, Row> orderActionRDD = sessionId2DetailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> v1) throws Exception {
                        Row row = v1._2();
                        return row.getString(8) != null;
                    }
                }
        );

        JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2();

                        String orderCategoryIds = row.getString(8);
                        String[] orderCategoryIdsSplit = orderCategoryIds.split(",");

                        List<Tuple2<Long, Long>> list = new ArrayList<>();

                        for (String orderCategoryId :
                                orderCategoryIdsSplit) {
                            list.add(new Tuple2<>(Long.valueOf(orderCategoryId), 1L));
                        }

                        return list;
                    }
                }
        );

        return orderCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );
    }

    private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2DetailRDD) {
        JavaPairRDD<String, Row> payActionRDD = sessionId2DetailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> v1) throws Exception {

                        Row row = v1._2();
                        String payCategoryId = row.getString(10);

                        return payCategoryId != null;
                    }
                }
        );

        JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> stringRowTuple2) throws Exception {
                        Row row = stringRowTuple2._2();
                        String payCategoryIds = row.getString(10);
                        String[] payCategoryIdsSplit = payCategoryIds.split(",");

                        List<Tuple2<Long, Long>> list = new ArrayList<>();
                        for (String payCategoryId :
                                payCategoryIdsSplit) {
                            list.add(new Tuple2<>(Long.valueOf(payCategoryId), 1L));
                        }

                        return list;
                    }
                }
        );

        return payCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );
    }

}
