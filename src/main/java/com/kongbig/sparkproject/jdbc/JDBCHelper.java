package com.kongbig.sparkproject.jdbc;

import com.kongbig.sparkproject.conf.ConfigurationManager;
import com.kongbig.sparkproject.constant.Constants;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Describe: JDBC辅助组件
 * Author:   kongbig
 * Data:     2018/1/31.
 */
public class JDBCHelper {

    private static final Logger LOGGER = Logger.getLogger(JDBCHelper.class);

    // 在静态代码块中，直接加载数据库的驱动
    static {
        try {
            String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    // 实现JDBCHelper的单例化
    private static JDBCHelper instance = null;
    // 自定义数据库连接池
    private LinkedList<Connection> dataSource = new LinkedList<Connection>();

    /**
     * 私有化构造方法
     */
    private JDBCHelper() {
        // 第一步，获取数据库连接池的大小
        Integer dataSourceSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);
        // 第二步：创建指定数量的数据库连接，并放入到数据库连接池中
        String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
        String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
        String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
        for (int i = 0; i < dataSourceSize; i++) {
            try {
                Connection conn = DriverManager.getConnection(url, user, password);
                dataSource.push(conn);
            } catch (SQLException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 两步检查机制的单例模式
     *
     * @return
     */
    public static JDBCHelper getInstance() {
        if (null == instance) {
            synchronized (JDBCHelper.class) {
                if (null == instance) {
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }

    /**
     * 提供获取数据库连接的方法
     * 有可能获取的时候，连接都被用光了，就暂时获取不到数据库连接
     * 实现一个简单的等待机制，去等待获取到数据库连接
     *
     * @return
     */
    public synchronized Connection getConnection() {
        while (dataSource.size() == 0) {
            try {
                // 无连接则等待10毫秒
                Thread.sleep(10);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
        return dataSource.poll();
    }

    /**
     * 执行增删改SQL语句
     *
     * @param sql
     * @param params
     * @return
     */
    public int executeUpdate(String sql, Object[] params) {
        int rtn = 0;
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
            rtn = pstmt.executeUpdate();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            if (conn != null) {
                // 归还连接给自定义数据库连接池
                dataSource.push(conn);
            }
        }
        return rtn;
    }

    /**
     * 执行查询sql语句
     *
     * @param sql
     * @param params
     * @param callback
     */
    public void executeQuery(String sql, Object[] params, QueryCallback callback) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);

            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }

            rs = pstmt.executeQuery();
            callback.process(rs);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            if (conn != null) {
                dataSource.push(conn);
            }
        }
    }

    /**
     * 批量执行SQL语句
     * <p>
     * 批量执行SQL语句，是JDBC中的一个高级功能
     * 默认情况下，每次执行一条SQL语句，就会通过网络连接，向MySQL发送一次请求
     * <p>
     * 但是，如果在短时间内要执行多条结构完全一模一样的SQL，只是参数不同
     * 虽然使用PreparedStatement这种方式，可以只编译一次SQL，提高性能，但是，还是对于每次SQL
     * 都要向MySQL发送一次网络请求
     * <p>
     * 可以通过批量执行SQL语句的功能优化这个性能
     * 一次性通过PreparedStatement发送多条SQL语句，比如100条、1000条，甚至上万条
     * 执行的时候，也仅仅编译一次就可以
     * 这种批量执行SQL语句的方式，可以大大提升性能
     *
     * @param sql
     * @param paramsList
     * @return 每条SQL语句影响的行数
     */
    public int[] executeBatch(String sql, List<Object[]> paramsList) {
        int[] rtn = null;
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = getConnection();
            // 第一步：使用Connection对象，取消自动提交
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sql);

            // 第二步：使用PreparedStatement.addBatch()方法加入批量的SQL参数
            for (Object[] params : paramsList) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
                pstmt.addBatch();
            }

            // 第三步：使用PreparedStatement.executeBatch()方法，执行批量的SQL语句
            rtn = pstmt.executeBatch();

            // 最后一步：
            conn.commit();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            if (conn != null) {
                dataSource.push(conn);
            }
        }
        return rtn;
    }

    /**
     * 静态内部类：查询回调接口
     */
    public static interface QueryCallback {
        /**
         * 处理查询结果
         *
         * @param rs
         * @throws Exception
         */
        void process(ResultSet rs) throws Exception;
    }

}
