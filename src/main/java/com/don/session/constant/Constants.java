package com.don.session.constant;

/**
 * 常量接口
 * Created by caoweidong on 2017/2/12.
 */
public interface Constants {

    String SPARK_LOCAL = "spark.local";

    /**
     * Spark 作业相关常量
     */
    String SPARK_APP_NAME_SESSION = "UserVisitSessionAnalyzeSpark";

    /**
     * 任务相关常量
     */
    //开始日期
    String PARAM_START_DATE = "startDate";

    //结束日期
    String PARAM_END_DATE = "endDate";


    String FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds";
    String FIELD_SEARCH_KEYWORDS = "searchKeywords";
    String FIELD_SESSION_ID = "sessionid";
    String FIELD_AGE = "age";
    String FIELD_PROFESSIONAL = "professional";
    String FIELD_CITY = "city";
    String FIELD_SEX = "sex";
    String FIELD_VISIT_LENGTH = "visitLength";
    String FIELD_STEP_LENGTH = "stepLength";
    String FIELD_START_TIME = "startTime";
    String FIELD_CLICK_COUNT = "clickCount";
    String FIELD_ORDER_COUNT = "orderCount";
    String FIELD_PAY_COUNT = "payCount";
    String FIELD_CATEGORY_ID = "categoryid";
    String SESSION_COUNT = "session_count";


    String TIME_PERIOD_1s_3s = "1s_3s";
    String TIME_PERIOD_4s_6s = "4s_6s";
    String TIME_PERIOD_7s_9s = "7s_9s";
    String TIME_PERIOD_10s_30s = "10s_30s";
    String TIME_PERIOD_30s_60s = "30s_60s";
    String TIME_PERIOD_1m_3m = "1m_3m";
    String TIME_PERIOD_3m_10m = "3m_10m";
    String TIME_PERIOD_10m_30m = "10m_30m";
    String TIME_PERIOD_30m = "30m";

    String STEP_PERIOD_1_3 = "1_3";
    String STEP_PERIOD_4_6 = "4_6";
    String STEP_PERIOD_7_9 = "7_9";
    String STEP_PERIOD_10_30 = "10_30";
    String STEP_PERIOD_30_60 = "30_60";
    String STEP_PERIOD_60 = "60";
    /**
     * 任务相关常量
     */
    String PARAM_START_AGE = "startAge";
    String PARAM_END_AGE = "endAge";
    String PARAM_PROFESSIONALS = "professionals";
    String PARAM_CITIES = "cities";
    String PARAM_SEX = "sex";
    String PARAM_KEYWORDS = "keywords";
    String PARAM_CATEGORY_IDS = "categoryIds";
}
