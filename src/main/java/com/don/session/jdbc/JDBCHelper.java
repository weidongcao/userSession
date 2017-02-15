package com.don.session.jdbc;


import com.don.session.conf.ConfigurationManager;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * JDBC辅助组件
 * 获取数据库连接，执行增删改查
 * Created by caoweidong on 2017/2/5.
 * @author caoweidong
 */
public class JDBCHelper {
    //加载驱动
    static{
        try {
            String driver = ConfigurationManager.getProperty(JDBCConstants.JDBC_DRIVER);
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 实现JDBCHelper的单例化
    private static JDBCHelper instance = null;



    public static JDBCHelper getInstance() {
        if (instance == null) {
            synchronized (JDBCHelper.class) {
                if (instance == null) {
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }

    //数据库连接池
    private LinkedList<Connection> datasource = new LinkedList<Connection>();

    /**
     * 私有化构造方法
     */
    private JDBCHelper() {
        // 第一步：获取数据库连接池的大小，就是说数据库连接池中要放多少个数据库连接
        int datasourceSize = ConfigurationManager.getInteger(JDBCConstants.JDBC_DATASOURCE_SIZE);

        //创建指定数量的数据库连接，并放入数据库连接池中
        for (int i = 0; i < datasourceSize; i++) {
            String url = ConfigurationManager.getProperty(JDBCConstants.JDBC_URL);
            String user = ConfigurationManager.getProperty(JDBCConstants.JDBC_USER);
            String password = ConfigurationManager.getProperty(JDBCConstants.JDBC_PASSWORD);

            try {
                Connection conn = DriverManager.getConnection(url, user, password);
                datasource.push(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 提供获取数据库连接的方法
     *
     */
    public synchronized Connection getConnection() {
        while (datasource.size() == 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }

    /**
     * 执行增删改SQL语句
     */

    public int executeUpdate(String sql, Object[] params) {
        int rtn = 0;
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);

            for(int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }

        return rtn;
    }

    /**
     * 执行查询SQL语句
     *
     * @param sql
     * @param params
     * @param callback
     */
    public void executeQuery(String sql, Object[] params, QueryCallback callback) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            for(int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }

            rs = pstmt.executeQuery();

            callback.process(rs);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }
    }

    /**
     * 指批量执行sql语句
     */
    public int[] executeBatch(String sql, List<Object[]> paramsList) {
        int[] rtn = null;
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = getConnection();

            //使用Connection对象，取消自动提交
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sql);

            //使用PreparedStatement.addBatch()方法加入指的sql参数
            for (Object[] params : paramsList) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
                pstmt.addBatch();
            }

            //使用PreparedStatement.executeBatch()方法，执行批量的SQL语句
            rtn = pstmt.executeBatch();

            //使用Connection对象指批量的SQL语句
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rtn;
    }

    /**
     * 静态内部类:查询回调接口
     * @author caoweidong
     */
    public static interface QueryCallback{
        /**
         * 处理查询结果
         * @param rs
         * @throws Exception
         */
        void process(ResultSet rs) throws Exception;
    }
}
