package com.don.session.dao.impl;

import com.don.session.dao.ITop10SessionDao;
import com.don.session.domain.Top10Session;
import com.don.session.jdbc.JDBCHelper;

/**
 * Created by Administrator on 2017-03-28.
 */
public class Top10SessionDaoImpl implements ITop10SessionDao {
    @Override
    public void insert(Top10Session session) {
        String sql = "insert into top10_session values(?, ?, ?, ?)";

        Object[] params = new Object[]{
                session.getTaskId(),
                session.getCategoryId(),
                session.getSessionId(),
                session.getClickCount()
        };

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
