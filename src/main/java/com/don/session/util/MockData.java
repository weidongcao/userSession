package com.don.session.util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.*;


/**
 * 模拟数据程序
 * @author Administrator
 *
 */
public class MockData {

	/**
	 * 模拟数据
	 * @param sc
	 * @param sqlContext
	 */
	public static void mock(JavaSparkContext sc,
							SQLContext sqlContext) {
		List<Row> rows = new ArrayList<Row>();

		//搜索关键词列表
		String[] searchKeywords = new String[]{"火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
				"呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉"};

		//生成日期
		String date = DateUtils.getTodayDate();

		//Action类型
		String[] actions = new String[]{"search", "click", "order", "pay"};

		//随机数类
		Random random = new Random();

		//100个User，每个User10个Session
		for(int i = 0; i < 100; i++) {
			//随机生成0~100的随机数作为Userid
			long userid = random.nextInt(100);

			//生成10 个Session
			for (int j = 0; j < 10; j++) {
				//通过UUID生成SessionID
				String sessionid = UUID.randomUUID().toString().replace("-", "");

				String baseActionTime = date + " " + random.nextInt(23);

				Long clickCategoryId = null;

				//每个Session的动作
				for (int k = 0; k < random.nextInt(100); k++) {
					long pageid = random.nextInt(10);
					String actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)));
					String searchKeyword = null;
					Long clickProductId = null;
					String orderCategoryIds = null;
					String orderProductIds = null;
					String payCategoryIds = null;
					String payProductIds = null;

					String action = actions[random.nextInt(4)];
					//随机生成动作类型
					if ("search".equals(action)) {//搜索操作
						searchKeyword = searchKeywords[random.nextInt(10)];
					} else if ("click".equals(action)) {//点击操作
						if (clickCategoryId == null) {
							clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));
						}
						clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));
					} else if ("order".equals(action)) {//下单
						orderCategoryIds = String.valueOf(random.nextInt(100));
						orderProductIds = String.valueOf(random.nextInt(100));
					} else if ("pay".equals(action)) {//支付
						payCategoryIds = String.valueOf(random.nextInt(100));
						payProductIds = String.valueOf(random.nextInt(100));
					}

					//生成数据
					Row row = RowFactory.create(date, userid, sessionid,
							pageid, actionTime, searchKeyword,
							clickCategoryId, clickProductId,
							orderCategoryIds, orderProductIds,
							payCategoryIds, payProductIds,
							Long.valueOf(String.valueOf(random.nextInt(10))));
					rows.add(row);
				}
			}
		}

		//生成的数据并行化到Spark
		JavaRDD<Row> rowsRDD = sc.parallelize(rows);

		//构造Spark SQL数据结构
		StructType schema = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("date", DataTypes.StringType, true),
				DataTypes.createStructField("user_id", DataTypes.LongType, true),
				DataTypes.createStructField("session_id", DataTypes.StringType, true),
				DataTypes.createStructField("page_id", DataTypes.LongType, true),
				DataTypes.createStructField("action_time", DataTypes.StringType, true),
				DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
				DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
				DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
				DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
				DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
				DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
				DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true),
				DataTypes.createStructField("city_id", DataTypes.LongType, true)));

		//生成DataFrame
		DataFrame df = sqlContext.createDataFrame(rowsRDD, schema);

		//注册为user_visit_action临时表
		df.registerTempTable("user_visit_action");
		for(Row _row : df.take(1)) {
			System.out.println(_row);
		}

		/**
		 * ==================================================================
		 */

		//生成查询条件数据
		rows.clear();
		String[] sexes = new String[]{"male", "female"};
		for(int i = 0; i < 100; i ++) {
			long userid = i;
			String username = "user" + i;
			String name = "name" + i;
			int age = random.nextInt(60);
			String professional = "professional" + random.nextInt(100);
			String city = "city" + random.nextInt(100);
			String sex = sexes[random.nextInt(2)];

			Row row = RowFactory.create(userid, username, name, age,
					professional, city, sex);
			rows.add(row);
		}

		//查询条件数据并行化到Spark
		rowsRDD = sc.parallelize(rows);

		//构造SparkSql数据结构
		StructType schema2 = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("user_id", DataTypes.LongType, true),
				DataTypes.createStructField("username", DataTypes.StringType, true),
				DataTypes.createStructField("name", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.IntegerType, true),
				DataTypes.createStructField("professional", DataTypes.StringType, true),
				DataTypes.createStructField("city", DataTypes.StringType, true),
				DataTypes.createStructField("sex", DataTypes.StringType, true)));

		//生成查询条件的DataFrame
		DataFrame df2 = sqlContext.createDataFrame(rowsRDD, schema2);
		for(Row _row : df2.take(1)) {
			System.out.println(_row);
		}

		//注册为user_info的临时表
		df2.registerTempTable("user_info");

		/**
		 * ==================================================================
		 */
		rows.clear();

		//生成产品状态数据
		int[] productStatus = new int[]{0, 1};

		for(int i = 0; i < 100; i ++) {
			long productId = i;
			String productName = "product" + i;
			String extendInfo = "{\"product_status\": " + productStatus[random.nextInt(2)] + "}";

			Row row = RowFactory.create(productId, productName, extendInfo);
			rows.add(row);
		}

		rowsRDD = sc.parallelize(rows);

		StructType schema3 = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("product_id", DataTypes.LongType, true),
				DataTypes.createStructField("product_name", DataTypes.StringType, true),
				DataTypes.createStructField("extend_info", DataTypes.StringType, true)));

		DataFrame df3 = sqlContext.createDataFrame(rowsRDD, schema3);
		for(Row _row : df3.take(1)) {
			System.out.println(_row);
		}

		df3.registerTempTable("product_info");
	}

}
