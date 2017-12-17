package kmeans;
/*kmeans工具类*/
import java.util.Vector;

import java.util.List;

public class Utils {
	//欧式距离
	public static double getDistance(Vector<Double> p1, Vector<Double> p2) {
		double sum = 0.0;
		for (int i = 0; i < p1.size(); i++)
			sum += Math.pow(p1.get(i) - p2.get(i), 2);
		return Math.sqrt(sum);
	}

	public static Vector<Double> str2Vector(String value) {
		String[] strs = value.split(",");
		Vector<Double> v = new Vector<>();
		for (String str : strs) {

			v.add(Double.parseDouble(str));
		}
		return v;
	}
	//使每次只加入销量价格两个属性
	public static Vector<Double> str2Vector2(String value) {
		String[] strs = value.split(",");
		Vector<Double> v = new Vector<>();
		v.add(Double.parseDouble(strs[2]));
		v.add(Double.parseDouble(strs[3]));

		return v;
	}

	public static String avgDistance(List<Vector<Double>>  datas, int size) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < size; i++) {
			double sum = 0.0;
			for (Vector<Double> v : datas)
				sum += v.get(i);
			double avg = sum / datas.size();
			sb.append(avg).append(",");
		}
		return sb.toString();
	}
}
