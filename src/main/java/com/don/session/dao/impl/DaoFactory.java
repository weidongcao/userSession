package com.don.session.dao.impl;


import com.don.session.dao.ISessionAggrStatDao;
import com.don.session.dao.ISessionDetailDao;
import com.don.session.dao.ISessionRandomExtractDao;
import com.don.session.dao.ITaskDao;

/**
   */
public class DaoFactory {

    /**
     * 获取任务管理
     */
    public static ITaskDao getTaskDAO() {
        return new TaskDaoImpl();
    }

    /**
     * 获取Sesison聚合统计DAO
     */
    public static ISessionAggrStatDao getSessionAggrStatDao() {
        return new SessionAggrStatDAOImpl();
    }

    public static ISessionRandomExtractDao getSessionRandomExtractDao() {
        return new SessionRandomExtractDaoImpl();
    }


    public static ISessionDetailDao getSessionDetailDao() {
        return new SessionDetailDaoImpl();
    }
}
