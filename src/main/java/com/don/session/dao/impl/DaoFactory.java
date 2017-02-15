package com.don.session.dao.impl;


import com.don.session.dao.ITaskDao;

/**
 * Created by caoweidong on 2017/2/11.
 */
public class DaoFactory {

    /**
     * 获取任务管理
     */
    public static ITaskDao getTaskDAO() {
        return new TaskDaoImpl();
    }

}
