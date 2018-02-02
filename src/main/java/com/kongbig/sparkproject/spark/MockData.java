package com.kongbig.sparkproject.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.kongbig.sparkproject.util.CustomStringUtils;
import com.kongbig.sparkproject.util.DateUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Describe: 模拟数据程序
 * Author:   kongbig
 * Data:     2018/1/31.
 */
public class MockData {

    /**
     * 模拟数据（模拟Hive表）
     *
     * @param sc
     * @param sqlContext
     */
    public static void mock(JavaSparkContext sc, SQLContext sqlContext) {
        List<Row> rows = new ArrayList<Row>();

        String[] searchKeywords = new String[]{"火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
                "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉"};
        String date = DateUtils.getTodayDate();
        // 用户行为：搜索、点击、下订单、支付
        String[] actions = new String[]{"search", "click", "order", "pay"};
        Random random = new Random();

        for (int i = 0; i < 100; i++) {
            long userId = random.nextInt(100);// 0-99随机数

            for (int j = 0; j < 10; j++) {
                String sessionId = UUID.randomUUID().toString().replace("-", "");
                String baseActionTime = date + " " + random.nextInt(23);// 0-22随机数

                Long clickCategoryId = null;

                for (int k = 0; k < random.nextInt(100); k++) {
                    long pageId = random.nextInt(10);
                    // actionTime:yyyy-MM-dd HH:mm:ss
                    String actionTime = baseActionTime + ":" + CustomStringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + CustomStringUtils.fulfuill(String.valueOf(random.nextInt(59)));
                    String searchKeyword = null;
                    Long clickProductId = null;
                    String orderCategoryIds = null;
                    String orderProductIds = null;
                    String payCategoryIds = null;
                    String payProductIds = null;
                    // 0-3:用户行为：搜索、点击、下订单、支付
                    String action = actions[random.nextInt(4)];
                    if ("search".equals(action)) {
                        searchKeyword = searchKeywords[random.nextInt(10)];
                    } else if ("click".equals(action)) {
                        if (clickCategoryId == null) {
                            clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));
                        }
                        clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));
                    } else if ("order".equals(action)) {
                        orderCategoryIds = String.valueOf(random.nextInt(100));
                        orderProductIds = String.valueOf(random.nextInt(100));
                    } else if ("pay".equals(action)) {
                        payCategoryIds = String.valueOf(random.nextInt(100));
                        payProductIds = String.valueOf(random.nextInt(100));
                    }

                    Row row = RowFactory.create(date, 
                            userId, 
                            sessionId,
                            pageId, 
                            actionTime, 
                            searchKeyword,
                            clickCategoryId, 
                            clickProductId,
                            orderCategoryIds, 
                            orderProductIds,
                            payCategoryIds, 
                            payProductIds,
                            Long.valueOf(String.valueOf(random.nextInt(10))));// 城市Id
                    rows.add(row);
                }
            }
        }

        // Distribute a local Scala collection to form an RDD.
        JavaRDD<Row> rowsRDD = sc.parallelize(rows);

        // 通过StructType直接指定Schema
        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),// 0
                DataTypes.createStructField("user_id", DataTypes.LongType, true),// 1
                DataTypes.createStructField("session_id", DataTypes.StringType, true),// 2
                DataTypes.createStructField("page_id", DataTypes.LongType, true),// 3
                DataTypes.createStructField("action_time", DataTypes.StringType, true),// 4
                DataTypes.createStructField("search_keyword", DataTypes.StringType, true),// 5
                DataTypes.createStructField("click_category_id", DataTypes.LongType, true),// 6
                DataTypes.createStructField("click_product_id", DataTypes.LongType, true),// 7
                DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),// 8
                DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),// 9
                DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),// 10
                DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true),// 11
                DataTypes.createStructField("city_id", DataTypes.LongType, true)));// 12

        DataFrame df = sqlContext.createDataFrame(rowsRDD, schema);
        
        // **注册成临时表
        df.registerTempTable("user_visit_action");
        for (Row _row : df.take(1)) {// 取出第一条记录打印
            System.out.println(_row);
        }

        /**
         * ==================================================================
         */

        rows.clear();
        String[] sexes = new String[]{"male", "female"};
        for (int i = 0; i < 100; i++) {
            long userId = i;
            String username = "user" + i;
            String name = "name" + i;
            int age = random.nextInt(60);// 0-59岁
            String professional = "professional" + random.nextInt(100);
            String city = "city" + random.nextInt(100);
            String sex = sexes[random.nextInt(2)];

            Row row = RowFactory.create(userId, 
                    username, 
                    name, 
                    age,
                    professional, 
                    city, 
                    sex);
            rows.add(row);
        }

        rowsRDD = sc.parallelize(rows);

        StructType schema2 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("user_id", DataTypes.LongType, true),// 0
                DataTypes.createStructField("username", DataTypes.StringType, true),// 1
                DataTypes.createStructField("name", DataTypes.StringType, true),// 2
                DataTypes.createStructField("age", DataTypes.IntegerType, true),// 3
                DataTypes.createStructField("professional", DataTypes.StringType, true),// 4
                DataTypes.createStructField("city", DataTypes.StringType, true),// 5
                DataTypes.createStructField("sex", DataTypes.StringType, true)));// 6

        DataFrame df2 = sqlContext.createDataFrame(rowsRDD, schema2);
        for (Row _row : df2.take(1)) {
            System.out.println(_row);
        }

        df2.registerTempTable("user_info");

        /**
         * ==================================================================
         */
        rows.clear();

        int[] productStatus = new int[]{0, 1};

        for (int i = 0; i < 100; i++) {
            long productId = i;
            String productName = "product" + i;
            String extendInfo = "{\"product_status\": " + productStatus[random.nextInt(2)] + "}";

            Row row = RowFactory.create(productId, 
                    productName, 
                    extendInfo);
            rows.add(row);
        }

        rowsRDD = sc.parallelize(rows);

        StructType schema3 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("product_id", DataTypes.LongType, true),
                DataTypes.createStructField("product_name", DataTypes.StringType, true),
                DataTypes.createStructField("extend_info", DataTypes.StringType, true)));

        DataFrame df3 = sqlContext.createDataFrame(rowsRDD, schema3);
        for (Row _row : df3.take(1)) {
            System.out.println(_row);
        }

        df3.registerTempTable("product_info");
    }

}
