package com.don.session.dao.factory;


import com.don.session.dao.*;
import com.don.session.dao.impl.*;

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

    public static ITop10CategoryDao getTop10CategoryDao() {
        return new Top10CategoryDaoImpl();
    }

    public static ITop10SessionDao getTop10SessionDao() {
        return new Top10SessionDaoImpl();
    }
}
