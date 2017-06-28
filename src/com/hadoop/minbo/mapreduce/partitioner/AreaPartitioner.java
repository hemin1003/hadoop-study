package com.hadoop.minbo.mapreduce.partitioner;

import java.util.HashMap;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分组实现
 * @param <KEY>
 * @param <VALUE>
 */
public class AreaPartitioner<KEY, VALUE> extends Partitioner<KEY, VALUE> {
	private static HashMap<String, Integer> areaMap = new HashMap<String, Integer>();
	static {
		areaMap.put("186", 0);
		areaMap.put("136", 1);
		areaMap.put("137", 2);
		areaMap.put("183", 3);
	}

	public int getPartition(KEY key, VALUE value, int numPartitions) {
		// 从key中拿到手机号，查询手机号归属地词典，不同省份返回不同的归属地号
		int areaCode = areaMap.get(key.toString().substring(0, 3)) == null ? 5
				: areaMap.get(key.toString().substring(0, 3));
		return areaCode;
	}
}
