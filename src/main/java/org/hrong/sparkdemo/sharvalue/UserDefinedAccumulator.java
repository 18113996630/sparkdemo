package org.hrong.sparkdemo.sharvalue;

import org.apache.spark.util.AccumulatorV2;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;

/**
 * @ClassName UserDefinedAccumulator
 * @Author hrong
 * @Date 2018/12/27 17:09
 * @Description 自定义AccumulatorV2
 * @Version 1.0
 **/
public class UserDefinedAccumulator extends AccumulatorV2<String, String> {

	private String one = "ONE";
	private String two = "TWO";
	private String three = "THREE";
	/**
	 * 将想要计算的字符串拼接起来，并赋初始值，后续针对data进行累加，并返回
	 */
	private String data = one + ":0;" + two + ":0;" + three + ":0;";
	/**
	 * 原始状态
	 */
	private String zero = data;

	/**
	 * 判断是否是初始状态，直接与原始状态的字符串进行对比
	 */
	@Override
	public boolean isZero() {
		return data.equals(zero);
	}

	/**
	 * 复制一个新的累加器
	 */
	@Override
	public AccumulatorV2<String, String> copy() {
		return new UserDefinedAccumulator();
	}

	/**
	 * 重置，恢复原始状态
	 */
	@Override
	public void reset() {
		data = zero;
	}

	/**
	 * 针对传入的字符串，与当前累加器现有的值进行累加
	 */
	@Override
	public void add(String v) {
		data = mergeData(v, data, ";");
	}

	/**
	 * 将两个累加器的计算结果进行合并
	 */
	@Override
	public void merge(AccumulatorV2<String, String> other) {
		Executors.newCachedThreadPool();
		data = mergeData(other.value(), data, ";");
	}

	/**
	 * 将此累加器的计算值返回
	 */
	@Override
	public String value() {
		return data;
	}

	/**
	 * 合并两个字符串
	 * @param data_1 字符串1
	 * @param data_2 字符串2
	 * @param delimit 分隔符
	 * @return 结果
	 */
	private String mergeData(String data_1, String data_2, String delimit) {
		StringBuffer res = new StringBuffer();
		String[] infos_1 = data_1.split(delimit);
		String[] infos_2 = data_2.split(delimit);
		Map<String, Integer> map_1 = new HashMap<>();
		Map<String, Integer> map_2 = new HashMap<>();
		for (String info : infos_1) {
			String[] kv = info.split(":");
			if (kv.length == 2) {
				String k = kv[0].toUpperCase();
				Integer v = Integer.valueOf(kv[1]);
				map_1.put(k, v);
			}
		}
		for (String info : infos_2) {
			String[] kv = info.split(":");
			if (kv.length == 2) {
				String k = kv[0].toUpperCase();
				Integer v = Integer.valueOf(kv[1]);
				map_2.put(k, v);
			}
		}
		for (Map.Entry<String, Integer> entry : map_1.entrySet()) {
			String key = entry.getKey();
			Integer value = entry.getValue();
			if (map_2.containsKey(key)) {
				value = value + map_2.get(key);
				map_2.remove(key);
			}
			res.append(key).append(":").append(value).append(delimit);
		}
		for (Map.Entry<String, Integer> entry : map_1.entrySet()) {
			String key = entry.getKey();
			Integer value = entry.getValue();
			if (res.toString().contains(key)) {
				continue;
			}
			res.append(key).append(":").append(value).append(delimit);
		}
		if (!map_2.isEmpty()) {
			for (Map.Entry<String, Integer> entry : map_2.entrySet()) {
				String key = entry.getKey();
				Integer value = entry.getValue();
				res.append(key).append(":").append(value).append(delimit);
			}
		}
		return res.toString().substring(0, res.toString().length() - 1);
	}

	/**
	 * 从给定字符串中获取指定key的值
	 * @param data 给定的字符串
	 * @param delimit 分隔符
	 * @param param key
	 * @return count
	 */
	private Integer getFieldFromData(String data, String delimit, String param) {
		Objects.requireNonNull(data, "data must not be null");
		try {
			String[] split = data.split(delimit);
			for (String infos : split) {
				String[] kv = infos.split(":");
				if (param.equalsIgnoreCase(kv[0])) {
					return Integer.valueOf(kv[1]);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private String setValue2Data(String data, String delimit, String k, String v) {
		StringBuilder res = new StringBuilder();
		try {
			String[] infos = data.split(delimit);
			for (String info : infos) {
				String[] kv = info.split(":");
				if (k.equalsIgnoreCase(kv[0])) {
					info = kv[0] + ":" + v;
				}
				res.append(info).append(delimit);
			}
			return res.toString().substring(0, res.toString().length() - 1);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
