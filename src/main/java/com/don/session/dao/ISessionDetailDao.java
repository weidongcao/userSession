package com.don.session.dao;

import com.don.session.domain.SessionDetail;

/**
 * Session明细DAO接口
 *
 * @author Cao Wei Dong on 2017-03-17.
 */
public interface ISessionDetailDao {

    /**
     * 插入一条数据
     * @param sessionDetail
     */
    void insert(SessionDetail sessionDetail);
}
