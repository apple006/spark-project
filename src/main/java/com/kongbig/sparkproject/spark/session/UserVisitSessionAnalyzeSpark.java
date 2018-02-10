package com.kongbig.sparkproject.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Optional;
import com.kongbig.sparkproject.conf.ConfigurationManager;
import com.kongbig.sparkproject.constant.Constants;
import com.kongbig.sparkproject.dao.*;
import com.kongbig.sparkproject.dao.impl.DAOFactory;
import com.kongbig.sparkproject.domain.*;
import com.kongbig.sparkproject.spark.MockData;
import com.kongbig.sparkproject.util.*;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
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
import scala.Tuple2;

import java.util.*;

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

    private static final Logger LOGGER = Logger.getLogger(UserVisitSessionAnalyzeSpark.class);

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
        JavaPairRDD<String, Row> sessionId2ActionRDD = getSessionId2ActionRDD(actionRDD);

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

        // 生成公共的RDD：通过筛选条件的session的访问明细数据
        JavaPairRDD<String, Row> sessionId2DetailRDD = getSessionId2DetailRDD(
                filteredSessionId2AggrInfoRDD, sessionId2ActionRDD);

        /**
         * 对于Accumulator这种分布式累加计算的变量的使用，有一个重要的说明：
         * 从Accumulator中，获取数据，插入数据库的时候，一定要是在有某一个action操作以后再进行。
         * 如果没有action的话，那么整个程序根本不会运行。
         *
         * 必须把能够触发job执行的操作，放在最终写入MySQL方法之前。
         * 计算出来的结果，在J2EE中，使用两张柱状图显示
         */
        // System.out.println(filteredSessionId2AggrInfoRDD.count());

        /**
         * 说明：要将上一个功能session聚合统计数据获取到，就必须是在一个action操作触发job之后才能从
         * Accumulator中获取数据，否则是获取不到数据的，因为没有job执行，Accumulator的值为空。
         * 所以，将随机抽取的功能的实现代码写在这里，放在session聚合统计功能的最终计算和写库之前，
         * 因为随机抽取功能中，有一个countByKey算子，是action操作，会出发job。
         */
        randomExtractSession(task.getTaskId(), filteredSessionId2AggrInfoRDD, sessionId2ActionRDD);

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

        // 获取top10热门品类
        List<Tuple2<CategorySortKey, String>> top10CategoryList =
                getTop10Category(task.getTaskId(), sessionId2DetailRDD);

        // 获取top10活跃session
        getTop10Session(sc, task.getTaskId(), top10CategoryList, sessionId2DetailRDD);

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
     * 获取sessionId到访问行为数据的映射的RDD
     *
     * @param actionRDD
     * @return
     */
    public static JavaPairRDD<String, Row> getSessionId2ActionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            private static final long serialVersionUID = 4730678416559888130L;

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2), row);
            }
        });
    }

    /**
     * 对行为数据按session粒度进行聚合(aggregate)
     *
     * @param sqlContext
     * @param actionRDD  actionRDD 行为数据RDD
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
                    private static final long serialVersionUID = -5830760044266343903L;

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
                    private static final long serialVersionUID = -2551197416671460542L;

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
                                + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                                + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);
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
                    private static final long serialVersionUID = -3590411703709291341L;

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
                    private static final long serialVersionUID = 5380181045404411060L;

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
     * @param taskParam
     * @param sessionAggrStatAccumulator
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
                    private static final long serialVersionUID = 2562792183846923954L;

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
     * 获取通过筛选条件的session的访问明细数据
     *
     * @param sessionId2AggrInfoRDD
     * @param sessionId2ActionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionId2DetailRDD(
            JavaPairRDD<String, String> sessionId2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionId2ActionRDD) {
        JavaPairRDD<String, Row> sessionId2DetailRDD = sessionId2AggrInfoRDD
                .join(sessionId2ActionRDD)
                .mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
                    private static final long serialVersionUID = 2954192244294999537L;

                    @Override
                    public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                        return new Tuple2<String, Row>(tuple._1, tuple._2._2);
                    }
                });
        return sessionId2DetailRDD;
    }

    /**
     * 随机抽取session
     *
     * @param sessionId2AggrInfoRDD
     */
    private static void randomExtractSession(
            final long taskId,
            JavaPairRDD<String, String> sessionId2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionId2ActionRDD) {
        /**
         * 1.计算每天每小时的session数量
         * 获取<yyyy-MM-dd_HH, aggrInfo>格式的RDD
         */
        JavaPairRDD<String, String> time2SessionIdRDD = sessionId2AggrInfoRDD.mapToPair(
                new PairFunction<Tuple2<String, String>, String, String>() {
                    private static final long serialVersionUID = 2954192244294999537L;

                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
                        String aggrInfo = tuple._2;
                        String startTime = CustomStringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_START_TIME);
                        String dateHour = DateUtils.getDateHour(startTime);
                        // <yyyy-MM-dd_HH, aggrInfo>
                        return new Tuple2<String, String>(dateHour, aggrInfo);
                    }
                });
        /**
         * 每天每小时的session数量，然后计算出每天每小时的session抽取索引，遍历每天每小时session
         * 首先抽取出的session的聚合数据，写入session_random_extract表
         * 所以第一个RDD的value应该是session聚合数据（数据全容易操作）
         */
        // 得到每天每小时的session数量
        Map<String, Object> countMap = time2SessionIdRDD.countByKey();

        /**
         * 2.使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引
         *
         * 将<yyyy-MM-dd_HH, count>格式的map，转换成<yyyy-MM-dd, <HH, count>>的格式
         * {2018-02-04={08=3, 09=4, 10=8, ...}}
         */
        Map<String, Map<String, Long>> dateHourCountMap = new HashMap<String, Map<String, Long>>();
        for (Map.Entry<String, Object> countEntry : countMap.entrySet()) {
            String dateHour = countEntry.getKey();
            long count = Long.valueOf(String.valueOf(countEntry.getValue()));

            String date = dateHour.split("_")[0];// yyyy-MM-dd 2018-02-04
            String hour = dateHour.split("_")[1];// HH 10
            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if (hourCountMap == null) {
                hourCountMap = new HashMap<String, Long>();
                dateHourCountMap.put(date, hourCountMap);
            }
            // 设置值
            hourCountMap.put(hour, count);
        }

        /**
         * 开始实现按时间比例随机抽取算法
         * 总共要抽取100个session，先按照天数，进行平分
         */
        int extractNumberPerDay = 100 / dateHourCountMap.size();// 每天抽取的session数（平均）
        // <date, <hour, (3, 5, 20, 102)>>
        final Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<String, Map<String, List<Integer>>>();
        Random random = new Random();

        for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
            String date = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

            // 计算出这一天的session总数
            long sessionCount = 0L;
            for (long hourCount : hourCountMap.values()) {
                sessionCount += hourCount;
            }

            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<String, List<Integer>>();
                dateHourExtractMap.put(date, hourExtractMap);
            }

            // 遍历每个小时
            for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();
                /**
                 * 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
                 * 就可以计算出当前小时需要抽取的session数量
                 */
                int hourExtractNumber = (int) ((double) count / (double) sessionCount * extractNumberPerDay);
                if (hourExtractNumber > count) {
                    hourExtractNumber = (int) count;
                }

                // 先获取当前小时存放随机数的list
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if (extractIndexList == null) {
                    extractIndexList = new ArrayList<Integer>();
                    hourExtractMap.put(hour, extractIndexList);
                }

                // 生成上面计算出来的数量的随机数
                for (int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt((int) count);
                    while (extractIndexList.contains(extractIndex)) {// 不能重复
                        extractIndex = random.nextInt((int) count);
                    }

                    extractIndexList.add(extractIndex);
                }
            }
        }

        /**
         * 3.遍历每天每小时的session，然后根据随机索引进行抽取
         */
        // 执行group算子，得到<dateHour, (session aggrInfo)> Map<String, List<String>>
        JavaPairRDD<String, Iterable<String>> time2SessionsRDD = time2SessionIdRDD.groupByKey();
        /**
         * 用flatMap算子遍历所有的<dateHour, (session aggrInfo)>格式的数据，会遍历每天
         * 每小时的session，如果发现某个session恰巧在我们指定的这天这小时的随机抽取索引上，
         * 那么抽取该session，直接写入MySQL的random_extract_session表。
         * 将抽取出来的session id返回回来，形成一个新的JavaRDD<String>，
         * 最后，使用抽取出来的sessionId，去join它们的访问行为明细数据，写入session_detail
         */
        JavaPairRDD<String, String> extractSessionIdsRDD = time2SessionsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
                    private static final long serialVersionUID = 297003405266678305L;

                    @Override
                    public Iterable<Tuple2<String, String>> call(
                            Tuple2<String, Iterable<String>> tuple) throws Exception {
                        List<Tuple2<String, String>> extractSessionIds = new ArrayList<Tuple2<String, String>>();

                        String dateHour = tuple._1;
                        String date = dateHour.split("_")[0];
                        String hour = dateHour.split("_")[1];
                        Iterator<String> iterator = tuple._2.iterator();

                        List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);
                        ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO();

                        int index = 0;
                        while (iterator.hasNext()) {
                            String sessionAggrInfo = iterator.next();
                            if (extractIndexList.contains(index)) {
                                String sessionId = CustomStringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
                                // 将数据写入MySQL
                                SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                                sessionRandomExtract.setTaskId(taskId);
                                sessionRandomExtract.setSessionId(sessionId);
                                sessionRandomExtract.setStartTime(CustomStringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));
                                sessionRandomExtract.setSearchKeywords(CustomStringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
                                sessionRandomExtract.setClickCategoryIds(CustomStringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));

                                sessionRandomExtractDAO.insert(sessionRandomExtract);

                                // 将sessionId写入list
                                extractSessionIds.add(new Tuple2<String, String>(sessionId, sessionId));
                            }
                            index++;
                        }
                        return extractSessionIds;
                    }
                });

        /**
         * 4.获取抽取出来的session的明细数据
         */
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD =
                extractSessionIdsRDD.join(sessionId2ActionRDD);
        extractSessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
            private static final long serialVersionUID = -1851055547113281924L;

            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                Row row = tuple._2._2;
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskId(taskId);
                sessionDetail.setUserId(row.getLong(1));
                sessionDetail.setSessionId(row.getString(2));
                sessionDetail.setPageId(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getLong(6));
                sessionDetail.setClickProductId(row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));
                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insert(sessionDetail);
            }
        });
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
                (double) visit_length_1s_3s / (double) session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double) visit_length_4s_6s / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double) visit_length_7s_9s / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double) visit_length_10s_30s / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double) visit_length_30s_60s / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double) visit_length_1m_3m / (double) session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double) visit_length_3m_10m / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_10m_30m / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_30m / (double) session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double) step_length_1_3 / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double) step_length_4_6 / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double) step_length_7_9 / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double) step_length_10_30 / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double) step_length_30_60 / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double) step_length_60 / (double) session_count, 2);

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

    /**
     * 获取top10热门品类
     *
     * @param taskId
     * @param sessionId2DetailRDD
     */
    private static List<Tuple2<CategorySortKey, String>> getTop10Category(
            long taskId,
            JavaPairRDD<String, Row> sessionId2DetailRDD) {
        /**
         * 1.获取符合条件的session访问过的所有品类
         */
        // 获取session访问过的所有品类id
        // 访问过：即点击过、下单过、支付过的品类
        JavaPairRDD<Long, Long> categoryIdRDD = sessionId2DetailRDD
                .flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    private static final long serialVersionUID = -3156797889348954404L;

                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();

                        Long clickCategoryId = row.getLong(6);
                        if (clickCategoryId != null) {
                            list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));
                        }

                        String orderCategoryIds = row.getString(8);
                        if (orderCategoryIds != null) {
                            String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                            for (String orderCategoryId : orderCategoryIdsSplited) {
                                list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId),
                                        Long.valueOf(orderCategoryId)));
                            }
                        }

                        String payCategoryIds = row.getString(10);
                        if (payCategoryIds != null) {
                            String[] payCategoryIdsSplited = payCategoryIds.split(",");
                            for (String payCategoryId : payCategoryIdsSplited) {
                                list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId),
                                        Long.valueOf(payCategoryId)));
                            }
                        }
                        return list;
                    }
                });
        /**
         * 必须要进行去重
         * 如果不去重的话，会出现重复的categoryId，排序会对重复的categoryId以及countInfo进行排序
         * 最后很可能会拿到重复的数据。
         */
        categoryIdRDD = categoryIdRDD.distinct();

        /**
         * 2.计算各品类的点击、下单和支付的次数
         * 访问明细中，其中三种访问行为是：点击、下单和支付
         * 分别来计算各品类点击、下单和支付的次数，可以现对访问明细数据进行过滤
         * 分别过滤出点击、下单和支付行为，然后通过map、reduceByKey等算子来进行计算
         */
        // 计算各个品类的点击次数
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionId2DetailRDD);

        // 计算各个品类的下单次数
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionId2DetailRDD);

        // 计算各个品类的支付次数
        JavaPairRDD<Long, Long> payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionId2DetailRDD);

        /**
         * 3.join各品类与它的点击、下单、支付的次数
         *
         * categoryIdRDD中，是包含了所有的符合条件的session，访问过的品类id
         *
         * 上面分别计算出来的三分，各品类的点击、下单和支付次数，可能不是包含所有品类的
         * 比如，有的品类，就只是被点击过，但是没有人下单和支付
         *
         * 所以，这里不能使用join操作，要使用leftOuterJoin操作，就是说，如果categoryIdRDD中不能
         * join到自己的某个数据，比如点击、或下单、或支付次数，那么该categoryIdRDD还是要保留下来的
         * 只不过，没有join到的那个数据，就是0了
         */
        JavaPairRDD<Long, String> categoryId2CountRDD = joinCategoryAndData(
                categoryIdRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD, payCategoryId2CountRDD);

        /**
         * 4.自定义二次排序key
         */

        /**
         * 5.将数据映射成<CategorySortKey, info>格式的RDD，然后进行二次排序（降序）
         */
        JavaPairRDD<CategorySortKey, String> sortKey2CountRDD = categoryId2CountRDD.mapToPair(
                new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {
                    private static final long serialVersionUID = -2002926188243478134L;

                    @Override
                    public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple) throws Exception {
                        String countInfo = tuple._2;
                        long clickCount = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                                countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
                        long orderCount = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                                countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
                        long payCount = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                                countInfo, "\\|", Constants.FIELD_PAY_COUNT));

                        CategorySortKey sortKey = new CategorySortKey(clickCount, orderCount, payCount);
                        return new Tuple2<CategorySortKey, String>(sortKey, countInfo);
                    }
                });
        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD =
                sortKey2CountRDD.sortByKey(false);// false:降序排序

        /**
         * 6.用take(10)取出top10热门品类，并写入MySQL
         */
        ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
        List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryCountRDD.take(10);
        for (Tuple2<CategorySortKey, String> tuple : top10CategoryList) {
            String countInfo = tuple._2;
            long categoryId = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
            long clickCount = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_PAY_COUNT));

            Top10Category category = new Top10Category(taskId, categoryId, clickCount, orderCount, payCount);
            top10CategoryDAO.insert(category);
        }
        return top10CategoryList;
    }

    /**
     * 获取各品类点击次数RDD
     *
     * @param sessionId2DetailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(
            JavaPairRDD<String, Row> sessionId2DetailRDD) {
        JavaPairRDD<String, Row> clickActionRDD = sessionId2DetailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    private static final long serialVersionUID = 825623610632401786L;

                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return row.get(6) != null;
                    }
                });

        JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(
                new PairFunction<Tuple2<String, Row>, Long, Long>() {
                    private static final long serialVersionUID = -8879608645470872310L;

                    @Override
                    public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
                        long clickCategoryId = tuple._2.getLong(6);
                        return new Tuple2<Long, Long>(clickCategoryId, 1L);
                    }
                });

        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    private static final long serialVersionUID = -313368598553346927L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });
        return clickCategoryId2CountRDD;
    }

    /**
     * 获取各品类的下单次数的RDD
     *
     * @param sessionId2DetailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(
            JavaPairRDD<String, Row> sessionId2DetailRDD) {
        JavaPairRDD<String, Row> orderActionRDD = sessionId2DetailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    private static final long serialVersionUID = -2250652177141838415L;

                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return row.getString(8) != null;
                    }
                });

        JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    private static final long serialVersionUID = -3843637800791163456L;

                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        String orderCategoryIds = row.getString(8);
                        String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();

                        for (String orderCategoryId : orderCategoryIdsSplited) {
                            list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));
                        }
                        return list;
                    }
                });

        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    private static final long serialVersionUID = 8846170248385442167L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });
        return orderCategoryId2CountRDD;
    }

    /**
     * 获取各个品类的支付次数RDD
     *
     * @param sessionId2DetailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(
            JavaPairRDD<String, Row> sessionId2DetailRDD) {
        JavaPairRDD<String, Row> payActionRDD = sessionId2DetailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    private static final long serialVersionUID = -2406496994996479021L;

                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return row.getString(10) != null;
                    }
                });

        JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    private static final long serialVersionUID = 993414743735613486L;

                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        String payCategoryIds = row.getString(10);
                        String[] payCategoryIdsSplited = payCategoryIds.split(",");
                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();

                        for (String payCategoryId : payCategoryIdsSplited) {
                            list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 1L));
                        }
                        return list;
                    }
                });

        JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    private static final long serialVersionUID = -432818160663825339L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });
        return payCategoryId2CountRDD;
    }

    /**
     * 连接品类RDD与数据RDD
     *
     * @param categoryIdRDD
     * @param clickCategoryId2CountRDD
     * @param orderCategoryId2CountRDD
     * @param payCategoryId2CountRDD
     * @return
     */
    private static JavaPairRDD<Long, String> joinCategoryAndData(
            JavaPairRDD<Long, Long> categoryIdRDD,
            JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
            JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
            JavaPairRDD<Long, Long> payCategoryId2CountRDD) {
        // 如果用leftOutJoin就可能出现右边那个RDD中，join过来时，没有值
        // 所以Tuple中第二个值用Optional<Long>类型，就代表，可能有值，可能没有值。
        JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD =
                categoryIdRDD.leftOuterJoin(clickCategoryId2CountRDD);

        JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple) throws Exception {
                        long categoryId = tuple._1;
                        Optional<Long> optional = tuple._2._2;
                        long clickCount = 0L;

                        if (optional.isPresent()) {// is有值
                            clickCount = optional.get();
                        }
                        String value = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|"
                                + Constants.FIELD_CLICK_COUNT + "=" + clickCount;
                        return new Tuple2<Long, String>(categoryId, value);
                    }
                });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                        long categoryId = tuple._1;
                        String value = tuple._2._1;
                        Optional<Long> optional = tuple._2._2;
                        long orderCount = 0L;

                        if (optional.isPresent()) {
                            orderCount = optional.get();
                        }
                        value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;
                        return new Tuple2<Long, String>(categoryId, value);
                    }
                });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                        long categoryId = tuple._1;
                        String value = tuple._2._1;

                        Optional<Long> optional = tuple._2._2;
                        long payCount = 0L;

                        if (optional.isPresent()) {
                            payCount = optional.get();
                        }
                        value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;
                        return new Tuple2<Long, String>(categoryId, value);
                    }
                });
        return tmpMapRDD;
    }

    /**
     * 获取top10活跃session
     *
     * @param taskId
     * @param sessionId2DetailRDD
     */
    private static void getTop10Session(
            JavaSparkContext sc,
            final long taskId,
            List<Tuple2<CategorySortKey, String>> top10CategoryList,
            JavaPairRDD<String, Row> sessionId2DetailRDD) {
        /**
         * 1.将top10热门品类的id，生成一份RDD
         */
        List<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<Tuple2<Long, Long>>();
        for (Tuple2<CategorySortKey, String> category : top10CategoryList) {
            long categoryId = Long.valueOf(CustomStringUtils.getFieldFromConcatString(
                    category._2, "\\|", Constants.FIELD_CATEGORY_ID));
            top10CategoryIdList.add(new Tuple2<Long, Long>(categoryId, categoryId));
        }

        JavaPairRDD<Long, Long> top10CategoryIdRDD =
                sc.parallelizePairs(top10CategoryIdList);

        /**
         * 2.计算top10品类被各session点击的次数
         */
        JavaPairRDD<String, Iterable<Row>> sessionId2DetailsRDD = sessionId2DetailRDD.groupByKey();
        JavaPairRDD<Long, String> categoryId2SessionCountRDD = sessionId2DetailsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                    @Override
                    public Iterable<Tuple2<Long, String>> call(
                            Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        String sessionId = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();

                        Map<Long, Long> categoryCountMap = new HashMap<Long, Long>();
                        // 计算出该session对每个品类的点击次数
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            if (row.get(6) != null) {// clickCategoryId
                                long categoryId = row.getLong(6);

                                Long count = categoryCountMap.get(categoryId);
                                if (count == null) {
                                    count = 0L;
                                }
                                count++;
                                categoryCountMap.put(categoryId, count);
                            }
                        }
                        // 返回结果 <categoryId, sessionId,count>格式
                        List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();

                        for (Map.Entry<Long, Long> categoryCountEntry : categoryCountMap.entrySet()) {
                            long categoryId = categoryCountEntry.getKey();
                            long count = categoryCountEntry.getValue();
                            String value = sessionId + "," + count;
                            list.add(new Tuple2<Long, String>(categoryId, value));
                        }
                        return list;
                    }
                });

        // 获取到top10热门品类，被各个session点击的次数
        JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryIdRDD
                .join(categoryId2SessionCountRDD)
                .mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> tuple) throws Exception {
                        return new Tuple2<Long, String>(tuple._1, tuple._2._2);
                    }
                });

        /**
         * 3.分组取TopN算法实现，获取每个品类的top10活跃用户
         */
        JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD =
                top10CategorySessionCountRDD.groupByKey();
        // <categoryId, ("sessionId1,count1", "sessionId2,count2")>
        JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {
                    @Override
                    public Iterable<Tuple2<String, String>> call(
                            Tuple2<Long, Iterable<String>> tuple) throws Exception {
                        long categoryId = tuple._1;
                        Iterator<String> iterator = tuple._2.iterator();
                        // 定义取TopN的数组
                        String[] top10Sessions = new String[10];
                        while (iterator.hasNext()) {
                            String sessionCount = iterator.next();
                            String sessionId = sessionCount.split(",")[0];
                            long count = Long.valueOf(sessionCount.split(",")[1]);

                            // 遍历排序数组
                            for (int i = 0; i < top10Sessions.length; i++) {
                                // 如果当前i位，没有数据，那么直接将i位数据赋值为当前sessionCount
                                if (top10Sessions[i] == null) {
                                    top10Sessions[i] = sessionCount;
                                    break;
                                } else {
                                    long _count = Long.valueOf(top10Sessions[i].split(",")[1]);
                                    // 如果sessionCount比i位的sessionCount要大
                                    if (count > _count) {
                                        // 从排序数组最后一位开始，到i位，所有数据往后挪一位
                                        // 从最后一位开始挪才行，否则会覆盖数据
                                        for (int j = 9; j > i; j--) {
                                            top10Sessions[j] = top10Sessions[j - 1];
                                        }
                                        // 将i位复制为sessionCount
                                        top10Sessions[i] = sessionCount;
                                        break;
                                    }
                                    // 比较小，继续外层循环
                                }
                            }
                        }

                        // 将数据写入MySQL表
                        ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
                        List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
                        for (String sessionCount : top10Sessions) {
                            if (sessionCount != null) {
                                String sessionId = sessionCount.split(",")[0];
                                long count = Long.valueOf(sessionCount.split(",")[1]);
                                // 将top10 session插入MySQL表
                                Top10Session top10Session = new Top10Session(taskId, categoryId, sessionId, count);
                                top10SessionDAO.insert(top10Session);

                                // 放入list
                                list.add(new Tuple2<String, String>(sessionId, sessionId));
                            }
                        }
                        return list;
                    }
                });

        /**
         * 4.获取top10活跃session的明细数据，并写入MySQL
         */
        JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD =
                top10SessionRDD.join(sessionId2DetailRDD);
        sessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                Row row = tuple._2._2;
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskId(taskId);
                sessionDetail.setUserId(row.getLong(1));
                sessionDetail.setSessionId(row.getString(2));
                sessionDetail.setPageId(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getLong(6));
                sessionDetail.setClickProductId(row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));
                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insert(sessionDetail);
            }
        });
    }

}
