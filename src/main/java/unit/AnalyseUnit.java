package unit;

public class AnalyseUnit {
	private String id;
	private int totalNum;
	private int impNum;
	private float rate;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public int getTotalNum() {
		return totalNum;
	}
	public void setTotalNum(int totalNum) {
		this.totalNum = totalNum;
	}
	public int getImpNum() {
		return impNum;
	}
	public void setImpNum(int impNum) {
		this.impNum = impNum;
	}
	@Override
	public String toString(){
		return id+"\t"+totalNum+"\t"+impNum+"\t"+rate;
	}
	public float getRate() {
		return rate;
	}
	public void setRate(float rate) {
		this.rate = rate;
	}
	
}
