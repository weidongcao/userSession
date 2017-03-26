package com.don.session.dao.impl;

import com.don.session.dao.ITop10CategoryDao;
import com.don.session.domain.Top10Category;
import com.don.session.jdbc.JDBCHelper;

/**
 * Created by Cao Wei Dong on 2017-03-26.
 */
public class Top10CategoryDaoImpl implements ITop10CategoryDao {
    @Override
    public void insert(Top10Category category) {
        String sql = "insert into top10_category values(?, ?, ?, ?, ?)";

        Object[] params = new Object[]{
                category.getTaskId(),
                category.getCategoryId(),
                category.getClickCount(),
                category.getOrderCount(),
                category.getPayCount()
        };

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
