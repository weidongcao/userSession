package com.don.session.dao;

import com.don.session.domain.Task;

/**
 * 任务管理DAO接口
 * Created by caoweidong on 2017/2/11.
 */
public interface ITaskDao {

    /**
     * 根据主键查询任务
     * @param taskid 主键
     * @return 任务
     */
    Task findById(long taskid);

}

