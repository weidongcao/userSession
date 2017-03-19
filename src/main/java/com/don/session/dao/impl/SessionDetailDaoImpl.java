package com.don.session.dao.impl;

import com.don.session.dao.ISessionDetailDao;
import com.don.session.domain.SessionDetail;
import com.don.session.jdbc.JDBCHelper;

/**
 * session明细DAO实现类
 *
 * @author Cao Wei Dong on 2017-03-17.
 */
public class SessionDetailDaoImpl implements ISessionDetailDao {

    /**
     * 插入一条Session明细数据
     * @param sessionDetail Session明细实体
     */
    @Override
    public void insert(SessionDetail sessionDetail) {
        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";

        Object[] params = new Object[]{
                sessionDetail.getTaskId(),
                sessionDetail.getUserId(),
                sessionDetail.getSessionId(),
                sessionDetail.getPageId(),
                sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyWord(),
                sessionDetail.getClickCategoryId(),
                sessionDetail.getClickProductId(),
                sessionDetail.getOrderCategoryIds(),
                sessionDetail.getOrderProductIds(),
                sessionDetail.getPayCategoryIds(),
                sessionDetail.getPayProductIds()
        };

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
