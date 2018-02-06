package com.kongbig.sparkproject.dao;

import com.kongbig.sparkproject.domain.SessionRandomExtract;

/**
 * Describe: session随机抽取模块DAO接口
 * Author:   kongbig
 * Data:     2018/2/6.
 */
public interface ISessionRandomExtractDAO {

    /**
     * 插入session随机抽取
     *
     * @param sessionRandomExtract
     */
    void insert(SessionRandomExtract sessionRandomExtract);

}
