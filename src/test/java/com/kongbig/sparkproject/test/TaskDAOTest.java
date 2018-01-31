package com.kongbig.sparkproject.test;

import com.kongbig.sparkproject.dao.ITaskDAO;
import com.kongbig.sparkproject.dao.impl.DAOFactory;
import com.kongbig.sparkproject.domain.Task;

/**
 * Describe: 任务管理DAO测试类
 * Author:   kongbig
 * Data:     2018/1/31.
 */
public class TaskDAOTest {

    public static void main(String[] args) {
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(1);
        System.out.println(task.getTaskName());
    }

}
