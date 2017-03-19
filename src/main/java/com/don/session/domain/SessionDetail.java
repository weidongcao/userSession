package com.don.session.domain;

/**
 * @author  Cao Wei Dong
 * date:2017年3月17日 17:39:52
 */
public class SessionDetail {

    private Long taskId;

    private Long userId;

    private String sessionId;

    private Long pageId;

    private String actionTime;

    private String searchKeyWord;

    private Long clickCategoryId;

    private Long clickProductId;

    private String orderCategoryIds;

    private String orderProductIds;

    private String payCategoryIds;

    private String payProductIds;

    public Long getTaskId() {
        return taskId;
    }

    public void setTaskId(Long taskId) {
        this.taskId = taskId;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public Long getPageId() {
        return pageId;
    }

    public void setPageId(Long pageId) {
        this.pageId = pageId;
    }

    public String getActionTime() {
        return actionTime;
    }

    public void setActionTime(String actionTime) {
        this.actionTime = actionTime;
    }

    public String getSearchKeyWord() {
        return searchKeyWord;
    }

    public void setSearchKeyWord(String searchKeyWord) {
        this.searchKeyWord = searchKeyWord;
    }

    public Long getClickCategoryId() {
        return clickCategoryId;
    }

    public void setClickCategoryId(Long clickCategoryId) {
        this.clickCategoryId = clickCategoryId;
    }

    public Long getClickProductId() {
        return clickProductId;
    }

    public void setClickProductId(Long clickProductId) {
        this.clickProductId = clickProductId;
    }

    public String getOrderCategoryIds() {
        return orderCategoryIds;
    }

    public void setOrderCategoryIds(String orderCategoryIds) {
        this.orderCategoryIds = orderCategoryIds;
    }

    public String getOrderProductIds() {
        return orderProductIds;
    }

    public void setOrderProductIds(String orderProductIds) {
        this.orderProductIds = orderProductIds;
    }

    public String getPayCategoryIds() {
        return payCategoryIds;
    }

    public void setPayCategoryIds(String payCategoryIds) {
        this.payCategoryIds = payCategoryIds;
    }

    public String getPayProductIds() {
        return payProductIds;
    }

    public void setPayProductIds(String payProductIds) {
        this.payProductIds = payProductIds;
    }
}
