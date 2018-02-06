package com.kongbig.sparkproject.dao.impl;

import com.kongbig.sparkproject.dao.ISessionAggrStatDAO;
import com.kongbig.sparkproject.dao.ISessionDetailDAO;
import com.kongbig.sparkproject.dao.ISessionRandomExtractDAO;
import com.kongbig.sparkproject.dao.ITaskDAO;

/**
 * Describe: 工厂类
 * Author:   kongbig
 * Data:     2018/1/31.
 */
public class DAOFactory {

    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }

    public static ISessionAggrStatDAO getSessionAggrStatDAO() {
        return new SessionAggrStatDAOImpl();
    }

    public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
        return new SessionRandomExtractDAOImpl();
    }

    public static ISessionDetailDAO getSessionDetailDAO(){
        return new SessionDetailDAOImpl();
    }
    
}
