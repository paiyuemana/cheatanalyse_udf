package cheat;

import java.io.IOException;
import java.math.BigDecimal;
//import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
//import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class LongTimeClick extends EvalFunc<String>{
	public Rate rate=new Rate();
	@Override
	public String exec(Tuple input) throws IOException{
		try{
			if(input==null||input.size()==0){
				return null;
			}
			Object input_obj=input.get(0);
			if(input_obj==null){
				return null;
			}
			if(!(input_obj instanceof DataBag)){
				int errCode = 2114;
	            String msg = "Expected input to be chararray, but" +
	            " got " + input_obj.getClass().getName();
	            throw new ExecException(msg, errCode, PigException.BUG);
			}
//			Map<String,Integer> uaCountMap=new HashMap<String,Integer>();
//			Map<String,Integer> ipCountMap=new HashMap<String,Integer>();
//			Map<String,Integer> geoidCountMap=new HashMap<String,Integer>();
			DataBag data_input=(DataBag)input_obj;
			Iterator<Tuple> it = data_input.iterator();
			long dataTotalNumber = data_input.size();
			long longtimeCount=0;
			while(it.hasNext()){
				Tuple Logs = it.next();
				List<Object> obj=Logs.getAll();
				if(obj.get(1)!=null){
					if(obj.get(1).toString().equals("LongTimeClick")){
						longtimeCount++;
					}
				}
			}
			float cheatRate=new BigDecimal(longtimeCount).divide(new BigDecimal(dataTotalNumber), 2, BigDecimal.ROUND_HALF_UP).floatValue();

//			return rate.getKeyAndBigRate(uaCountMap,dataTotalNumber)+"\t"+rate.getKeyAndBigRate(ipCountMap,dataTotalNumber)
//					+"\t"+rate.getKeyAndBigRate(geoidCountMap,dataTotalNumber);
			return longtimeCount+"\t"+dataTotalNumber+"\t"+cheatRate;
		}catch(ExecException e){
			throw e;
		}
	}
	
}
