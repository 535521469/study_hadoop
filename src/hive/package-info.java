/**
 * @author Administrator
 * 源数据模拟体彩排列五，每秒钟开一次奖
 * 一行一条记录，如下格式：
 * 20130101000000:9,3,6,3,8
 * 
 *	create [external] table lottery_origin (
 *		time String , 
 *		nums String) 
 *  partitioned by (year int)
 *	row format delimited 
 *  fields terminated by ':'
 *  lines terminated by '\n'
 *  ;
 *	
 * load data inpath 'hdfs:///tmp/2012' [overwrite] into table 
 *	
 *
 */
package hive;
