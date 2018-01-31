package com.kongbig.sparkproject.dao.impl;

import com.kongbig.sparkproject.dao.ITaskDAO;

/**
 * Describe: 工厂类
 * Author:   kongbig
 * Data:     2018/1/31.
 */
public class DAOFactory {

    /**
     * 获取任务管理DAO
     *
     * @return
     */
    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }

}
