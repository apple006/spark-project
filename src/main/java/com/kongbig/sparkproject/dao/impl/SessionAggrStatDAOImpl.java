package com.kongbig.sparkproject.dao.impl;

import com.kongbig.sparkproject.dao.ISessionAggrStatDAO;
import com.kongbig.sparkproject.domain.SessionAggrStat;
import com.kongbig.sparkproject.jdbc.JDBCHelper;

/**
 * Describe: session聚合统计DAO实现类
 * Author:   kongbig
 * Data:     2018/2/3.
 */
public class SessionAggrStatDAOImpl implements ISessionAggrStatDAO {

    @Override
    public void insert(SessionAggrStat sessionAggrStat) {
        String sql = "insert into session_aggr_stat values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        Object[] params = new Object[]{sessionAggrStat.getTaskId(),
                sessionAggrStat.getSession_count(),
                sessionAggrStat.getVisit_length_1s_3s_ratio(),
                sessionAggrStat.getVisit_length_4s_6s_ratio(),
                sessionAggrStat.getVisit_length_7s_9s_ratio(),
                sessionAggrStat.getVisit_length_10s_30s_ratio(),
                sessionAggrStat.getVisit_length_30s_60s_ratio(),
                sessionAggrStat.getVisit_length_1m_3m_ratio(),
                sessionAggrStat.getVisit_length_3m_10m_ratio(),
                sessionAggrStat.getVisit_length_10m_30m_ratio(),
                sessionAggrStat.getVisit_length_30m_ratio(),
                sessionAggrStat.getStep_length_1_3_ratio(),
                sessionAggrStat.getStep_length_4_6_ratio(),
                sessionAggrStat.getStep_length_7_9_ratio(),
                sessionAggrStat.getStep_length_10_30_ratio(),
                sessionAggrStat.getStep_length_30_60_ratio(),
                sessionAggrStat.getStep_length_60_ratio()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

}
