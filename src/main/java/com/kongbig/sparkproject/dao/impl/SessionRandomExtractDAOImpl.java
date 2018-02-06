package com.kongbig.sparkproject.dao.impl;

import com.kongbig.sparkproject.dao.ISessionRandomExtractDAO;
import com.kongbig.sparkproject.domain.SessionRandomExtract;
import com.kongbig.sparkproject.jdbc.JDBCHelper;

/**
 * Describe: 随机抽取session的DAO实现
 * Author:   kongbig
 * Data:     2018/2/6.
 */
public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {
    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql = "insert into session_random_extract values(?,?,?,?,?)";
        Object[] params = new Object[]{sessionRandomExtract.getTaskId(),
                sessionRandomExtract.getSessionId(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getSearchKeywords(),
                sessionRandomExtract.getClickCategoryIds()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
