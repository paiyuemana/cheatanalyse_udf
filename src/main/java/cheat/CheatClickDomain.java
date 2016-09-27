package cheat;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class CheatClickDomain extends EvalFunc<String>{
	public Rate rate=new Rate();
	@Override
	public String exec(Tuple input) throws IOException{
		if(input == null || input.size() == 0){
			return null;
		}
		try{
			Object log=input.get(0);
			if(log==null){
				return null;
			}
			if(!(log instanceof DataBag)){
				int errCode = 2114;
	            String msg = "Expected input to be databag, but" +
	            " got " + log.getClass().getName();
	            throw new ExecException(msg, errCode, PigException.BUG);
			}
//			Map<String,Integer> uaCountMap=new HashMap<String,Integer>();
//			Map<String,Integer> ipCountMap=new HashMap<String,Integer>();
//			Map<String,Integer> geoidCountMap=new HashMap<String,Integer>();
			DataBag data_input=(DataBag)log;
			Iterator<Tuple> it = data_input.iterator();
			long dataTotalNumber = data_input.size();
			if(dataTotalNumber<50){
				return null;
			}
			long repeatLongtimeCount=0;
			while(it.hasNext()){
				Tuple Logs = it.next();
				List<Object> obj=Logs.getAll();
				if(obj.get(1)!=null){
					if(obj.get(1).toString().equals("RepeatClick")){
						repeatLongtimeCount++;
					}
				}
			}
//			float cheatRate=new BigDecimal(repeatLongtimeCount*100).divide(new BigDecimal(dataTotalNumber), 2, BigDecimal.ROUND_HALF_UP).floatValue();
			float cheatRate=new BigDecimal(repeatLongtimeCount).divide(new BigDecimal(dataTotalNumber), 2, BigDecimal.ROUND_HALF_UP).floatValue();

//			return "(repeat+longtime)/total:"+repeatLongtimeCount+"/"+dataTotalNumber+"="+cheatRate+"\t"+rate.getKeyAndBigRate(uaCountMap,dataTotalNumber)+"\t"+rate.getKeyAndBigRate(ipCountMap,dataTotalNumber)
//					+"\t"+rate.getKeyAndBigRate(geoidCountMap,dataTotalNumber);
			return repeatLongtimeCount+"\t"+dataTotalNumber+"\t"+cheatRate;
		}catch(ExecException e){
			throw e;
		}
	}
}
