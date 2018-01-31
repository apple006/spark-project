package com.kongbig.sparkproject.test;

import com.kongbig.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Describe: JDBC辅助组件测试类
 * Author:   kongbig
 * Data:     2018/1/31.
 */
public class JDBCHelperTest {

    public static void main(String[] args) throws Exception {
        // 获取JDBCHelper的单例
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

//        test1(jdbcHelper);
//        test2(jdbcHelper);
        test3(jdbcHelper);

    }

    /**
     * 测试普通的增删改语句
     *
     * @param jdbcHelper
     */
    private static void test1(JDBCHelper jdbcHelper) {
        jdbcHelper.executeUpdate("insert into test_user(name, age) values(?,?)", new Object[]{"张三", 28});
    }

    /**
     * 测试普通查询语句
     *
     * @param jdbcHelper
     */
    private static void test2(JDBCHelper jdbcHelper) {
        final Map<String, Object> testUser = new HashMap<String, Object>();
        jdbcHelper.executeQuery("select name, age from test_user where id = ?",
                new Object[]{1},
                new JDBCHelper.QueryCallback() {
                    @Override
                    public void process(ResultSet rs) throws Exception {
                        if (rs.next()) {
                            String name = rs.getString(1);
                            int age = rs.getInt(2);
                            /**
                             * 匿名内部类如果要访问外部类中的一些成员，比如方法内的局部变量，
                             * 那么必须将局部变量，声明为final类型，才可以访问。
                             */
                            testUser.put("name", name);
                            testUser.put("age", age);
                        }
                    }
                });
        System.out.println(testUser.get("name") + " : " + testUser.get("age"));
    }

    /**
     * 测试批量执行SQL语句
     *
     * @param jdbcHelper
     */
    private static void test3(JDBCHelper jdbcHelper) {
        String sql = "insert into test_user(name, age) values(?, ?)";
        List<Object[]> paramsList = new ArrayList<Object[]>();
        paramsList.add(new Object[]{"李四", 30});
        paramsList.add(new Object[]{"王五", 25});

        jdbcHelper.executeBatch(sql, paramsList);
    }

}
