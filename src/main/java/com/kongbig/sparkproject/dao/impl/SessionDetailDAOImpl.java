package com.kongbig.sparkproject.dao.impl;

import com.kongbig.sparkproject.dao.ISessionDetailDAO;
import com.kongbig.sparkproject.domain.SessionDetail;
import com.kongbig.sparkproject.jdbc.JDBCHelper;

/**
 * Describe: session明细DAO实现类
 * Author:   kongbig
 * Data:     2018/2/6.
 */
public class SessionDetailDAOImpl implements ISessionDetailDAO {

    @Override
    public void insert(SessionDetail sessionDetail) {
        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
        Object[] params = new Object[]{sessionDetail.getTaskId(),
                sessionDetail.getUserId(),
                sessionDetail.getSessionId(),
                sessionDetail.getPageId(),
                sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyword(),
                sessionDetail.getClickCategoryId(),
                sessionDetail.getClickProductId(),
                sessionDetail.getOrderCategoryIds(),
                sessionDetail.getOrderProductIds(),
                sessionDetail.getPayCategoryIds(),
                sessionDetail.getPayProductIds()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
    
}
