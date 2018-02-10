package com.kongbig.sparkproject.dao.impl;

import com.kongbig.sparkproject.dao.ITop10SessionDAO;
import com.kongbig.sparkproject.domain.Top10Session;
import com.kongbig.sparkproject.jdbc.JDBCHelper;

/**
 * Describe: top10活跃session的DAO实现
 * Author:   kongbig
 * Data:     2018/2/10.
 */
public class Top10SessionDAOImpl implements ITop10SessionDAO {

    @Override
    public void insert(Top10Session top10Session) {
        String sql = "insert into top10_session values(?,?,?,?)";

        Object[] params = new Object[]{top10Session.getTaskId(),
                top10Session.getCategoryId(),
                top10Session.getSessionId(),
                top10Session.getClickCount()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

}
