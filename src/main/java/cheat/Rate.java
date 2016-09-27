package cheat;

import java.math.BigDecimal;
import java.util.Map;

public class Rate {
	public String getKeyAndBigRate(Map<String,Integer> map,long dataTotalNumber){
		int bigNumber=0;
		String nameOfBigNumber="";
		for(Map.Entry<String, Integer> entry : map.entrySet()){
			if(entry.getValue()>bigNumber){
				nameOfBigNumber=entry.getKey();
				bigNumber=entry.getValue();
			}
		}
		float rate=new BigDecimal(bigNumber*100).divide(new BigDecimal(dataTotalNumber), 2, BigDecimal.ROUND_HALF_UP).floatValue();
		return nameOfBigNumber+"\t"+bigNumber+"/"+dataTotalNumber+"="+rate;
	}
}
