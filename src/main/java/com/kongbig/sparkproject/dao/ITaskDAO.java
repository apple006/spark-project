package com.kongbig.sparkproject.dao;

import com.kongbig.sparkproject.domain.Task;

/**
 * Describe: 任务管理DAO接口
 * Author:   kongbig
 * Data:     2018/1/31.
 */
public interface ITaskDAO {

    /**
     * 根据主键查询任务
     *
     * @param taskId 主键
     * @return 任务
     */
    Task findById(long taskId);

}
