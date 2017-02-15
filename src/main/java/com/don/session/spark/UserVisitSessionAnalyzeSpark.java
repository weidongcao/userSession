package com.don.session.spark;

import com.don.session.conf.ConfigurationManager;
import com.don.session.constant.Constants;
import com.don.session.dao.ITaskDao;
import com.don.session.dao.impl.DaoFactory;
import com.don.session.domain.Task;
import com.don.session.util.MockData;
import com.don.session.util.ParamUtils;
import com.don.session.util.StringUtils;
import com.don.session.util.ValidUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.json.JSONException;
import org.json.JSONObject;
import scala.Tuple2;

import java.util.Iterator;

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
 * Created by caoweidong on 2017/2/12.
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
        args = new String[]{"1"};
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
        long taskid = ParamUtils.getLongFromArgs(args);

        Task task = taskDao.findById(taskid);

        JSONObject taskParam = null;
        try {
            taskParam = new JSONObject(task.getTaskParam());
        } catch (JSONException e) {
            e.printStackTrace();
        }

        //如果要进行Session粒度的数据聚合
        //要从User_visit_action表中查询出来指定日期范围内的行为数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);

        //首先可以将数据按照Session_id进行GroupByKey分组
        //此时的数据的粒度就是session粒度了，然后呢，可以将sesion粒度的数据与用户信息数据
        //进行join
        //然后就可以获取到Session粒度的数据，同时呢，数据里面还包含了Sesison对应的User的信息
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(sqlContext, actionRDD);

        //接着，就要针对Session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
        //相当于我们自己编写的牌子，是要访问外面的任务参数对象的
        //匿名内部类（牌子函数）访问外部对象是要给外部对象使用final修饰的
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD =
                filterSession(sessionid2AggrInfoRDD, taskParam);







        //关闭Spark上下文
        sc.close();
    }

    /**
     * 获取SQLContext
     * 如果是在本地测试环境的话，那么生成SQLContext对象
     * 如果是在生产环境运行的话，那么就生成HiveContext对象
     *
     * @param sc
     * @return
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
     * @param sqlContext
     * @param actionRDD 行为数据RDD
     * @return
     */
    private static JavaPairRDD<String, String> aggregateBySession(SQLContext sqlContext, JavaRDD<Row> actionRDD) {
        //ActionRDD中的元素是Row，一个Row就是一行用户访问行为记录，比如一次点击或者搜索
        //将Row映射成<sessionid,Row>的格式
        JavaPairRDD<String, Row> sessionid2ActionRDD = actionRDD.mapToPair(
                /**
                 * PairFunction
                 * 第一个参数相当于是函数的输入
                 * 第二个参数和第三个参数相当于是函数的输出Tuple)，分别是Tuple第一个和第二个值
                 */
                new PairFunction<Row, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        return new Tuple2<String, Row>(row.getString(2), row);
                    }

                }
        );

        //对行为数据按Session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2ActionRDD.groupByKey();

        //对每一个Session分组进行聚合，将Session中所有的搜索词和点击品类都聚合起来
        //到此为止，获取的数据格式如下 ：<userid, partAggrInfo(sessionid, searchKeywords, clickCategoryIds)>
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {

                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        String sessionid = tuple._1;
                        Iterator<Row> iter = tuple._2.iterator();

                        StringBuffer searchKeywordsBuffer = new StringBuffer("");
                        StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                        Long userid = null;

                        //遍历Session所有的访问行为
                        while (iter.hasNext()) {
                            //提取每个访问行为的搜索词字段和点击品类字段
                            Row row = iter.next();
                            if (userid == null) {
                                userid = row.getLong(1);
                            }
                            String searchKeyword = row.getString(5);
                            Long clickCategoryId = null;
                            try {
                                clickCategoryId = row.getLong(6);
                            } catch (Exception e) {
                            }

                            //并不是每一行访问行为都有SearchKeyword和ClickCategoryId两个字段的
                            //其实，只有搜索行为是有SearchKeyword字段的
                            //只有点击品类的行为，是有ClickCategoryId字段的
                            //所以，任何一行行为数据，都不可能两个字段都有，所有数据是可能出现null值的

                            //我们6决定是否将搜索词或者点击品类ID拼接到字符串中去
                            //首先要满足：不能是null值、
                            //其次，之前的字符串中还没有搜索词或者点击品类id

                            if (StringUtils.isNotEmpty(searchKeyword)) {
                                if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                                    searchKeywordsBuffer.append(searchKeyword + ",");
                                }
                            }

                            if (clickCategoryId != null) {
                                if (!clickCategoryIdsBuffer.toString().contentEquals(
                                        String.valueOf(clickCategoryId)
                                )) {
                                    clickCategoryIdsBuffer.append(clickCategoryId + ",");
                                }
                            }
                        }

                        String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                        //我们返回的数据格式，即<sessionid, partAggrInfo>
                        //但是，这一步聚合完了以后，其实，我们是还需要将每一行数据跟对应的用户信息进行进行聚合
                        //问题就来了，如果是跟用户信息进行聚合的话，那么key就不应该是sessionid
                        //就应该是userid，才能够跟<userid, Row>格式的用户信息进行聚合
                        //如果我们这里直接返回<session, partAggrInfo>, 还得再做一次MapToPair算子
                        //将RDD映射成<userid, parAggrInfo>的格式，那么就多此一举

                        //所以，这里其实可以直接返回的数据格式就是<userid, partAggrInfo>
                        //然后跟用户信息join的时候，将partAggrInfo关联上userInfo
                        //然后南直接将返回的Tuple的Key设置成sessionid
                        //然后再直接将返回的Tuple的Key设置成Sessionid
                        //最后的数据格式还是<sessionid, fullAggrInfo>

                        //聚合数据用什么样的格式进行拼接？
                        //我们这里统一定义，使用key=value|key=value
                        String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                                + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                                + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds;

                        return new Tuple2<Long, String>(userid, partAggrInfo);
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
                        return new Tuple2<Long, Row>(row.getLong(0), row);
                    }
                }
        );

        //将session粒度聚合数据，与用户信息进行join
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);

        //对join起来的数据进行拼接，并且返回<sessionid, fullAggrInfo> 格式的数据
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                        String partAggrInfo = tuple._2._1;
                        Row userInfoRow = tuple._2._2;

                        String sessionid = StringUtils.getValueFromStringByName(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                        int age = userInfoRow.getInt(3);

                        //用户职业
                        String professional = userInfoRow.getString(4);
                        //用户所在城市
                        String city = userInfoRow.getString(5);
                        //用户性别
                        String sex = userInfoRow.getString(6);

                        //
                        String fullAggrInfo = partAggrInfo + "|"
                                + Constants.FIELD_AGE + "=" + age + "|"
                                + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                                + Constants.FIELD_CITY + "=" + city + "|"
                                + Constants.FIELD_SEX + "=" + sex;
                        return new Tuple2<String, String>(sessionid, fullAggrInfo);
                    }
                }
        );
        return sessionid2FullAggrInfoRDD;
    }

    /**
     * 过滤Session数据
     * @param sessionid2AggrInfoRDD
     * @param taskParam
     * @return
     */
    private static JavaPairRDD<String,String> filterSession(JavaPairRDD<String, String> sessionid2AggrInfoRDD, JSONObject taskParam) {
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
        JavaPairRDD<String, String> filterdSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        //首先从tuple中获取聚合数据
                        String aggrInfo = tuple._2();

                        //接着，合资按照筛选条件进行过滤
                        //按照年龄范围进行过滤（startAge， endAge）
                        if (!ValidUtils.validBetween(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }

                        //按照职业范围进行过滤(professionals)
                        //互联网、IT、软件
                        if (!ValidUtils.validIn(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }

                        //按照老友记范围进行过滤(cities)
                        //北京， 上海， 广州， 深圳
                        //成都
                        if (!ValidUtils.validEqual(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        //按照点击品类id进行过滤
                        if (!ValidUtils.validIn(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        //按照搜索词进行过滤
                        //Session可能搜索了，火锅、蛋糕、等
                        //筛选条件可能是火锅、串串香、iphone
                        //那么，in这个校验方法主要判定Session搜索的词中有任何一个与筛选条件中任何一个搜索词相当，即通过
                        if (!ValidUtils.validIn(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        //按照点击品类id进行过滤
                        if (!ValidUtils.validIn(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }

                        return true;
                    }
                }
        );
        return filterdSessionid2AggrInfoRDD;
    }

}
