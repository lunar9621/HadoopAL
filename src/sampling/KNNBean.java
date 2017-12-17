package sampling;
//测试集
public class KNNBean implements Comparable<KNNBean>{
	private double type;
	private double distance;
	
	public KNNBean(){}
	public KNNBean(String str){
		String[] strs = str.split(":");
		type = Double.parseDouble(strs[0]);
		distance = Double.parseDouble(strs[1]);
	}
	public KNNBean(int type, double distance){
		this.type= type;
		this.distance = distance;
	}
	public Double getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	public double getDistance() {
		return distance;
	}
	public void setDistance(double distance) {
		this.distance = distance;
	}
	
	@Override
	public int compareTo(KNNBean o) {
		if(this.distance > o.distance){
			return 1;
		}else if(this.distance < o.distance){
			return -1;
		}
		return 0;
	}
	public String toString(){
		return type+" : "+distance;
	}
}
