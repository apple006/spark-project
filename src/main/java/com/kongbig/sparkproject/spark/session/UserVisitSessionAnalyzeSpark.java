package com.kongbig.sparkproject.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.kongbig.sparkproject.conf.ConfigurationManager;
import com.kongbig.sparkproject.constant.Constants;
import com.kongbig.sparkproject.dao.ISessionAggrStatDAO;
import com.kongbig.sparkproject.dao.ITaskDAO;
import com.kongbig.sparkproject.dao.impl.DAOFactory;
import com.kongbig.sparkproject.domain.SessionAggrStat;
import com.kongbig.sparkproject.domain.Task;
import com.kongbig.sparkproject.spark.MockData;
import com.kongbig.sparkproject.util.*;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.Accumulator;
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
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;

/**
 * Describe: 用户访问session分析Spark作业
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * 1、时间范围：起始日期~结束日期
 * 2、性别：男/女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 * <p>
 * Spark作业如何接受用户创建的任务？
 * 1.J2EE平台在接收到用户创建任务的请求之后，会将任务信息插入MySQL的task表中，任务参数以JSON格式封装在task_param字段中
 * 2.接着J2EE平台会执行我们的spark-submit shell脚本，并将taskId作为参数传递给spark-submit shell脚本
 * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数，传递给spark作业的main函数
 * 参数就封装在main函数的args数组中（这是spark本身提供的特性）
 * <p>
 * Author:   kongbig
 * Data:     2018/1/31.
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
        args = new String[]{"1"};

        // 构建Spark上下文
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSQLContext(sc.sc());// 从JavaSparkContext取出对应的SparkContext

        // 生成模拟测试数据
        mockData(sc, sqlContext);

        // 创建需要使用的DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        // 那么就首先得查询出来指定的任务
        long taskId = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDAO.findById(taskId);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());// json串得到jsonObj

        /**
         * 要进行session粒度的数据聚合：
         * 首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
         */
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);

        /**
         * 首先，可以将行为数据，按照session_id进行groupByKey分组，此时的数据的粒度就是session粒度了。
         * 然后，可以将session粒度的数据与用户信息数据，进行join，就获取到session粒度的数据（包含session对应的user的信息）
         */
        // <sessionId, (sessionId,searchKeywords,clickCategoryIds,age,professinal,city,sex)>
        JavaPairRDD<String, String> sessionId2AggrInfoRDD = aggregateBySession(sqlContext, actionRDD);

        /**
         * 针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤。
         * 相当于自己编写的算子，是要访问外面的任务参数对象的。
         * 匿名内部类（算子函数），访问外部对象，是要给外部对象使用final修饰的。
         */
        // 重构：同时进行过滤和统计
        Accumulator<String> sessionAggrStatAccumulator
                = sc.accumulator("", new SessionAggrStatAccumulator());
        JavaPairRDD<String, String> filteredSessionId2AggrInfoRDD = filterSessionAndAggrStat(
                sessionId2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);

        /**
         * 对于Accumulator这种分布式累加计算的变量的使用，有一个重要的说明：
         * 从Accumulator中，获取数据，插入数据库的时候，一定要是在有某一个action操作以后再进行。
         * 如果没有action的话，那么整个程序根本不会运行。
         * 
         * 必须把能够触发job执行的操作，放在最终写入MySQL方法之前。
         * 计算出来的结果，在J2EE中，使用两张柱状图显示
         */
        System.out.println(filteredSessionId2AggrInfoRDD.count());
        
        // 计算各session范围占比，并持久化聚合统计结果（MySQL）
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), taskId);

        /**
         * session聚合统计（统计出访问时长和访问步长，各个区间的session数量占总session数量的比例）
         *
         * 如果不进行重构，直接来实现，思路：
         * 1、actionRDD，映射成<sessionId, Row>的格式
         * 2、按sessionId聚合，计算出每个session的访问时长和访问步长，生成一个新的RDD
         * 3、遍历新生成的RDD，将每个session的访问时长和访问步长，去更新自定义Accumulator中的对应的值
         * 4、使用自定义Accumulator中的统计值，去计算各个区间的比例
         * 5、将最后计算出来的结果，写入MySQL对应的表中
         *
         * 普通实现思路的问题：
         * 1、之前session聚合已做过映射，再用actionRDD会多此一举
         * 2、是不是一定要为了session的聚合这个功能，单独去遍历一边session？无必要，已有session数据
         *      之前过滤session的时候，其实就相当于是在遍历session，那么这里没必要。
         *
         *  重构实现思路：
         *  1、不要去生成任何新的RDD（可能上亿数据量）
         *  2、不要去单独遍历一遍session的数据（可能处理上千万数据量）
         *  3、可以在进行session聚合的时候，就直接计算出来每个session的访问时长和访问步长
         *  4、在进行过滤的时候，本来就要遍历所有的聚合session信息，此时就可以在某个session通过筛选条件后
         *      将其访问时长和访问步长，累加到自定义的Accumulator上面去
         *  5、两种方式，在面对上亿，上千万数据的时候，甚至可以节省时间长达半小时-几小时
         *
         *  开发Spark大型复杂项目的一些经验准则：
         *  1、尽量少生成RDD
         *  2、尽量少对RDD进行算子操作，如果有可能，尽量在一个算子里面，实现多个需要做的功能
         *  3、尽量少对RDD进行shuffle算子操作，比如groupByKey、reduceByKey、sortByKey
         *      shuffle操作，会导致大量的磁盘读写，严重降低性能
         *      有shuffle的算子，和没有shuffle的算子，甚至性能，会达到几十分钟-数个小时的差别
         *      有shuffle的算子，很容易导致数据倾斜（性能杀手）
         *  4、无论做什么功能，性能第一
         *      在传统J2EE架构/可维护性/可扩展性高于性能
         *      在大数据项目中，比如MR、Hive、Spark、Storm，性能最重要（第一位）
         *      所以推荐大数据项目，在开发和代码的架构中，有限考虑性能；其次考虑功能代码的划分、解耦合
         */

        // 关闭Spark上下文
        sc.close();
    }

    /**
     * 获取SQLContext
     * 如果是在本地测试环境的话，那么就生成SQLContext对象
     * 如果是在生产环境运行的话，那么就生成HiveContext对象
     *
     * @param sc
     * @return
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            // HiveContext是SQLContext的子类
            return new HiveContext(sc);
        }
    }

    /**
     * 生成模拟数据（只有本地模式，才会去生成模拟数据）
     *
     * @param sc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc, sqlContext);
        }
    }

    /**
     * 获取指定日期范围内的用户访问行为数据
     *
     * @param sqlContext SQLContext
     * @param taskParam  任务参数
     * @return 行为数据RDD
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
        String sql = "select * from user_visit_action " +
                "where date >= '" + startDate + "' " +
                "and date <= '" + endDate + "'";
        DataFrame actionDF = sqlContext.sql(sql);
        return actionDF.javaRDD();
    }

    /**
     * 对行为数据按session粒度进行聚合(aggregate)
     *
     * @param actionRDD 行为数据RDD
     * @return session粒度聚合数据
     */
    private static JavaPairRDD<String, String> aggregateBySession(
            SQLContext sqlContext, final JavaRDD<Row> actionRDD) {
        // 现在actionRDD中的元素是Row，一个Row就是一行用户访问行为记录(如:一次点击或搜索)
        // 1.需要将这个Row映射成<sessionId, Row>的格式
        JavaPairRDD<String, Row> sessionId2ActionRDD = actionRDD.mapToPair(
                /**
                 * PairFunction
                 * 参数1:相当于函数的输入
                 * 参数2和参数3:相当于是函数的输出（Tuple），分别是Tuple的第一、第二个值
                 */
                new PairFunction<Row, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        // MockData类中临时表user_visit_action第3个数是sessionId
                        return new Tuple2<String, Row>(row.getString(2), row);
                    }
                });

        // 对行为数据按session粒度进行分组
        // key是sessionId，即按sessionId进行了分组
        JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD
                = sessionId2ActionRDD.groupByKey();

        // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        // 到此，数据格式如下：<userId, partAggrInfo(sessionId,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long, String> userId2PartAggrInfoRDD = sessionId2ActionsRDD.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        String sessionId = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();

                        // 搜索过的关键词
                        StringBuffer searchKeywordsBuffer = new StringBuffer("");
                        // 点击过的品类id
                        StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                        Long userId = null;

                        // session的起始和结束时间
                        Date startTime = null;
                        Date endTime = null;
                        // session的访问步长
                        int stepLength = 0;

                        // 遍历session所有的访问行为
                        while (iterator.hasNext()) {
                            // 提取每个访问行为的搜索词字段和点击品类字段(临时表user_visit_action)
                            Row row = iterator.next();
                            if (null == userId) {
                                userId = row.getLong(1);
                            }
                            String searchKeyword = row.getString(5);
                            Long clickCategoryId = row.getLong(6);
                            /**
                             * 说明：并不是每一行数据都有searchKeyword和clickCategoryId字段
                             * 只有搜索行为是有searchKeyword字段；
                             * 只有点击品类行为是有clickCategoryId字段；
                             *
                             * 是否拼接到buffer的条件：1.非空;2.之前的字符串中还没有搜索词或者点击品类id
                             */
                            if (StringUtils.isNotEmpty(searchKeyword)) {
                                if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                                    searchKeywordsBuffer.append(searchKeyword + ",");
                                }
                            }
                            if (null != clickCategoryId) {
                                if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
                                    clickCategoryIdsBuffer.append(clickCategoryId + ",");
                                }
                            }

                            // 计算session开始和结束时间
                            Date actionTime = DateUtils.parseTime(row.getString(4));

                            if (null == startTime) {
                                startTime = actionTime;
                            }
                            if (null == endTime) {
                                endTime = actionTime;
                            }

                            if (actionTime.before(startTime)) {
                                startTime = actionTime;
                            }
                            if (actionTime.after(endTime)) {
                                endTime = actionTime;
                            }

                            // 计算session访问步长
                            stepLength++;
                        }

                        String searchKeywords = CustomStringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickCategoryIds = CustomStringUtils.trimComma(clickCategoryIdsBuffer.toString());

                        // 计算session访问时长（秒）
                        long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

                        /**
                         * 返回的数据格式，即<sessionId, partAggrInfo>
                         * 可以直接返回的<userId, partAggrInfo>格式数据，然后直接跟用户信息join，
                         * 将partAggrInfo关联上userInfo，然后直接将返回的Tuple的key设置成sessionId
                         * 最后的数据格式<sessionId, fullAggrInfo>
                         */

                        /**
                         * 聚合数据进行拼接的格式：
                         * 统一定义：key=value|key=value
                         */
                        String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                                + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                                + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                                + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                                + Constants.FIELD_STEP_LENGTH + "=" + stepLength;
                        // 参数1:session对应的userId; 参数2:session对应的部分聚合数据
                        return new Tuple2<Long, String>(userId, partAggrInfo);
                    }
                });
        // 查询所有用户数据，并映射成<userId, Row>的格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();

        // 数据格式如下<userId, oneRowUserInfo>
        JavaPairRDD<Long, Row> userId2InfoRDD = userInfoRDD.mapToPair(
                new PairFunction<Row, Long, Row>() {
                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        // 临时表user_info中第1个字段为user_id
                        return new Tuple2<Long, Row>(row.getLong(0), row);
                    }
                });
        // 将session粒度聚合数据，与用户信息进行join
        // 即<userId, session部分聚合信息> join <userId, 用户信息>
        JavaPairRDD<Long, Tuple2<String, Row>> userId2FullInfoRDD
                = userId2PartAggrInfoRDD.join(userId2InfoRDD);

        // 对join起来的数据进行拼接，并且返回<sessionId, fullAggrInfo>格式的数据
        JavaPairRDD<String, String> sessionId2FullAggrInfoRDD = userId2FullInfoRDD.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                        String partAggrInfo = tuple._2._1;// 是拼接好的
                        Row userInfoRow = tuple._2._2;

                        String sessionId = CustomStringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                        int age = userInfoRow.getInt(3);
                        String professional = userInfoRow.getString(4);
                        String city = userInfoRow.getString(5);
                        String sex = userInfoRow.getString(6);

                        String fullAggrInfo = partAggrInfo + "|"
                                + Constants.FIELD_AGE + "=" + age + "|"
                                + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                                + Constants.FIELD_CITY + "=" + city + "|"
                                + Constants.FIELD_SEX + "=" + sex;

                        return new Tuple2<String, String>(sessionId, fullAggrInfo);
                    }
                }
        );
        return sessionId2FullAggrInfoRDD;
    }

    /**
     * 过滤session数据，并进行聚合统计
     *
     * @param sessionId2AggrInfoRDD
     * @return
     */
    private static JavaPairRDD<String, String> filterSessionAndAggrStat(
            JavaPairRDD<String, String> sessionId2AggrInfoRDD, final JSONObject taskParam,
            final Accumulator<String> sessionAggrStatAccumulator) {
        // 为了使用后面的ValidUtils，所以，这里将所有的筛选参数拼接成一个连接串
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professional = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORD);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        StringBuilder sb = new StringBuilder("");
        sb.append(StringUtils.isNotEmpty(startAge) ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "");
        sb.append(StringUtils.isNotEmpty(endAge) ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "");
        sb.append(StringUtils.isNotEmpty(professional) ? Constants.PARAM_PROFESSIONALS + "=" + professional + "|" : "");
        sb.append(StringUtils.isNotEmpty(cities) ? Constants.PARAM_CITIES + "=" + cities + "|" : "");
        sb.append(StringUtils.isNotEmpty(sex) ? Constants.PARAM_SEX + "=" + sex + "|" : "");
        sb.append(StringUtils.isNotEmpty(keywords) ? Constants.PARAM_KEYWORD + "=" + keywords + "|" : "");
        sb.append(StringUtils.isNotEmpty(categoryIds) ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds + "|" : "");
        String _parameter = (null != sb && sb.length() > 0 ? sb.toString() : "");

        if (_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;

        // 根据筛选参数进行过滤
        JavaPairRDD<String, String> filterSessionId2AggrInfoRDD = sessionId2AggrInfoRDD.filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        // 过滤逻辑如下
                        // 1.从tuple中获取聚合数据
                        String aggrInfo = tuple._2;
                        // 2.依次按照筛选条件进行过滤
                        // 2.1按照年龄范围进行过滤（startAge、endAge）
                        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter,
                                Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }
                        // 2.2按照职业范围进行过滤（professional）
                        // 互联网,IT,软件
                        // 互联网
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter,
                                Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }

                        // 2.3按照城市范围进行过滤（cities）
                        // 北京,上海,广州,深圳
                        // 成都
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter,
                                Constants.PARAM_CITIES)) {
                            return false;
                        }

                        // 2.4按照性别进行过滤
                        // 男/女
                        if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter,
                                Constants.PARAM_SEX)) {
                            return false;
                        }

                        // 2.5按照搜索词进行过滤
                        // 我们session可能搜索了火锅,蛋糕,烧烤
                        // 我们的筛选条件可能是火锅,串串香,iphone手机
                        // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
                        // 任何一个搜索词相等，即通过！
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
                                parameter, Constants.PARAM_KEYWORD)) {
                            return false;
                        }

                        // 2.6按照点击品类id进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
                                parameter, Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }

                        /**
                         * 如果经过了之前的多个过滤条件之后，程序能走到这里
                         * 那么就说明该session是通过了用户指定的筛选条件，也就是需要保留的session
                         * 那么就要对session的访问时长和访问步长进行统计，根据session对应的范围
                         * 进行相应的累加计数
                         */
                        // 走到这一步就是需要计算的session
                        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
                        // 计算出session的访问时长和访问步长，并进行相应的累加
                        long visitLength = Long.valueOf(CustomStringUtils
                                .getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                        long stepLength = Long.valueOf(CustomStringUtils
                                .getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
                        calculateVisitLength(visitLength);
                        calculateStepLength(stepLength);
                        return true;
                    }

                    /**
                     * 计算访问时长范围
                     *
                     * @param visitLength
                     */
                    private void calculateVisitLength(long visitLength) {
                        if (visitLength >= 1 && visitLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                        } else if (visitLength >= 4 && visitLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        } else if (visitLength >= 7 && visitLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        } else if (visitLength >= 10 && visitLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        } else if (visitLength > 30 && visitLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        } else if (visitLength > 60 && visitLength <= 180) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        } else if (visitLength > 180 && visitLength <= 600) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                        } else if (visitLength > 600 && visitLength <= 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        } else if (visitLength > 1800) {// 大于30分钟
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }
                    }

                    /**
                     * 计算访问步长范围
                     *
                     * @param stepLength
                     */
                    private void calculateStepLength(long stepLength) {
                        if (stepLength >= 1 && stepLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        } else if (stepLength >= 4 && stepLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        } else if (stepLength >= 7 && stepLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        } else if (stepLength >= 10 && stepLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        } else if (stepLength > 30 && stepLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        } else if (stepLength > 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }
                    }
                });

        return filterSessionId2AggrInfoRDD;
    }

    /**
     * 计算各session范围占比，并持久化聚合统计结果
     *
     * @param value
     */
    private static void calculateAndPersistAggrStat(String value, long taskId) {
        // 从Accumulator统计串中获取值
        long session_count = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.SESSION_COUNT));

        long visit_length_1s_3s = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围（比例），要将long转为double，不然小数会变为0
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double)visit_length_1s_3s / (double)session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double)visit_length_4s_6s / (double)session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double)visit_length_7s_9s / (double)session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double)visit_length_10s_30s / (double)session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double)visit_length_30s_60s / (double)session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double)visit_length_1m_3m / (double)session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double)visit_length_3m_10m / (double)session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_10m_30m / (double)session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_30m / (double)session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double)step_length_1_3 / (double)session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double)step_length_4_6 / (double)session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double)step_length_7_9 / (double)session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double)step_length_10_30 / (double)session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double)step_length_30_60 / (double)session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double)step_length_60 / (double)session_count, 2);

        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskId(taskId);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        // 调用对应的DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }

}
