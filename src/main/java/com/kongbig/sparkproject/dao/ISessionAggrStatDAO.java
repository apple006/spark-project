package com.kongbig.sparkproject.dao;

import com.kongbig.sparkproject.domain.SessionAggrStat;

/**
 * Describe: session聚合模块DAO接口
 * Author:   kongbig
 * Data:     2018/2/3.
 */
public interface ISessionAggrStatDAO {

    /**
     * 插入session聚合统计结果
     *
     * @param sessionAggrStat
     */
    void insert(SessionAggrStat sessionAggrStat);

}
