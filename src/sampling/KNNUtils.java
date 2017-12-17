package sampling;

public class KNNUtils {

	public static double getDistance(Point p1, Point p2){
		double sum = 0.0;
		for(int i=0;i<p1.getV().size();i++){
			sum += Math.pow(p1.getV().get(i) - p2.getV().get(i),2);
	}
		return Math.sqrt(sum);
	}
}
