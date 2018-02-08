package com.kongbig.sparkproject.dao.impl;

import com.kongbig.sparkproject.dao.ITop10CategoryDAO;
import com.kongbig.sparkproject.domain.Top10Category;
import com.kongbig.sparkproject.jdbc.JDBCHelper;

/**
 * Describe: top10品类DAO实现
 * Author:   kongbig
 * Data:     2018/2/8.
 */
public class Top10CategoryDAOImpl implements ITop10CategoryDAO {

    @Override
    public void insert(Top10Category category) {
        String sql = "insert into top10_category values(?,?,?,?,?)";

        Object[] params = new Object[]{category.getTaskId(),
                category.getCategoryId(),
                category.getClickCount(),
                category.getOrderCount(),
                category.getPayCount()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

}
