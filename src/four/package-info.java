/**
 * 
 */
/**
 * @author Administrator
 * 源数据模拟体彩排列五，每秒钟开一次奖
 * 一行一条记录，如下格式：
 * 20130101000000:9,3,6,3,8
 * 
 * 要求如下，有2个文件，分别是2013，2014
 * 需要把这两个文件作为SequenceFile来处理
 * 这些文件大小都超过了默认的块大小，原则上不需要使用SequenceFile类型来处理，不过SequenceFile有压缩。可以一试
 * 结果是两个66.4M的文件，经过压缩后得到30.9M的seq 文件
 * 
 * 先把两个文件写成SequenceFile，后从这个SequenceFile读取
 * 
 * 
 * 
 */
package four;