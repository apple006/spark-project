package com.kongbig.sparkproject.dao;

import com.kongbig.sparkproject.domain.Top10Category;

/**
 * Describe: top10品类DAO接口
 * Author:   kongbig
 * Data:     2018/2/8.
 */
public interface ITop10CategoryDAO {

    /**
     * 插入
     *
     * @param category
     */
    void insert(Top10Category category);

}
