package com.don.session.dao.impl;

import com.don.session.dao.ISessionRandomExtractDao;
import com.don.session.domain.SessionRandomExtract;
import com.don.session.jdbc.JDBCHelper;

/**
 * Created by Administrator on 2017/3/11.
 */
public class SessionRandomExtractDaoImpl implements ISessionRandomExtractDao{
    @Override
    public void insert(SessionRandomExtract session) {
        String sql = "insert into session_random_extract values(?, ?, ?, ?, ?)";

        Object[] params = new Object[]{
                session.getTaskId(),
                session.getSessionId(),
                session.getStartTime(),
                session.getSearchKeywords(),
                session.getClickCategoryIds()
        };
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
