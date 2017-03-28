package com.don.session.domain;

import java.io.Serializable;

/**
 * 指定品类下的Top10活跃用户
 * Created by Administrator on 2017-03-28.
 */
public class Top10Session implements Serializable {
    private long taskId;
    private long categoryId;

    private String sessionId;

    private long clickCount;


    public Top10Session(long taskId, long categoryId, String sessionId, long clickCount) {
        this.taskId = taskId;
        this.categoryId = categoryId;
        this.sessionId = sessionId;
        this.clickCount = clickCount;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(long categoryId) {
        this.categoryId = categoryId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }
}
