package sampling;

import java.util.Vector;
//训练集
public class Point {
	private double type;
	private Vector<Double> v = new Vector<>();
	private String value;
	
	public Point(){}
	public Point(String value){
		this.value = value;
		String[] strs = value.split(",");
		int index =0;
		for( ; index < strs.length-1;index++){//加入每个点的属性值
			v.add(Double.parseDouble(strs[index]));
		}
		type = Double.parseDouble(strs[index]); 
	}
	
	public double getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
		
	}
	public Vector<Double> getV() {
		return v;
	}
	public void setV(Vector<Double> v) {
		this.v = v;
	}
	
	public String toString(){
		return value;
	}
	
}
