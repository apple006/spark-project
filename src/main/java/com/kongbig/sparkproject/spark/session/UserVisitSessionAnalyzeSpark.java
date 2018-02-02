package com.kongbig.sparkproject.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.kongbig.sparkproject.conf.ConfigurationManager;
import com.kongbig.sparkproject.constant.Constants;
import com.kongbig.sparkproject.dao.ITaskDAO;
import com.kongbig.sparkproject.dao.impl.DAOFactory;
import com.kongbig.sparkproject.domain.Task;
import com.kongbig.sparkproject.spark.MockData;
import com.kongbig.sparkproject.util.CustomStringUtils;
import com.kongbig.sparkproject.util.ParamUtils;
import com.kongbig.sparkproject.util.ValidUtils;
import org.apache.commons.lang.StringUtils;
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
        System.out.println("聚合后数据总数：" + sessionId2AggrInfoRDD.count());
        // 打印聚合后的数据的前十条
        for (Tuple2<String, String> tuple : sessionId2AggrInfoRDD.take(10)) {
            System.out.println(tuple._2);
        }

        /**
         * 针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤。
         * 相当于自己编写的算子，是要访问外面的任务参数对象的。
         * 匿名内部类（算子函数），访问外部对象，是要给外部对象使用final修饰的。
         */
        JavaPairRDD<String, String> filteredSessionId2AggrInfoRDD
                = filterSession(sessionId2AggrInfoRDD, taskParam);
        System.out.println("过滤后数据总数：" + filteredSessionId2AggrInfoRDD.count());
        // 打印过滤后的数据的前十条
        for (Tuple2<String, String> tuple : filteredSessionId2AggrInfoRDD.take(10)) {
            System.out.println(tuple._2);
        }

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
            SQLContext sqlContext, JavaRDD<Row> actionRDD) {
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
                        }

                        String searchKeywords = CustomStringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickCategoryIds = CustomStringUtils.trimComma(clickCategoryIdsBuffer.toString());

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
                                + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds;
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
     * 过滤session数据
     *
     * @param sessionId2AggrInfoRDD
     * @return
     */
    private static JavaPairRDD<String, String> filterSession(
            JavaPairRDD<String, String> sessionId2AggrInfoRDD, final JSONObject taskParam) {
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

                        return true;
                    }
                });

        return filterSessionId2AggrInfoRDD;
    }

}
