package cheat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import unit.AnalyseUnit;

public class PossibleCheatDomain extends EvalFunc<DataBag>{
	private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
    private static final BagFactory BAG_FACTORY = BagFactory.getInstance();
	@Override
	public DataBag exec(Tuple input) throws IOException {
//		DataBag output = BAG_FACTORY.newDefaultBag();
		try{
			if (input == null || input.size() == 0) {
			      return null;
			}
			Object o = input.get(0);
			Object o2 = input.get(1);
			if(o==null||o2==null){
				return null;
			}
			if(!(o instanceof DataBag)||!(o2 instanceof String)){
				int errCode = 2114;
	            String msg = "Expected input to be chararray, but" +
	            " got " + o.getClass().getName();
	            throw new ExecException(msg, errCode, PigException.BUG);
			}
			DataBag id_domain_rate=(DataBag)o;
			String flag=(String)o2;
			int totalIndex=0;
			if("tid".equals(flag)||"idfa".equals(flag)){
				totalIndex=2;
			}else if("id".equals(flag)||"imei".equals(flag)){
				totalIndex=1;
			}else{
				return null;
			}
			Map<String,List<AnalyseUnit>> possibleCheatDomain=getPossibleCheat(id_domain_rate,totalIndex);
			if(possibleCheatDomain==null||possibleCheatDomain.isEmpty()){
				return null;
			}
			
			return map2DataBag(possibleCheatDomain);
			
		}catch(ExecException e){
			throw e;
		}
	}
	//获取DataBag中有作弊嫌疑的媒体和对应的id
	private Map<String,List<AnalyseUnit>> getPossibleCheat(DataBag id_domain_rate,int totalIndex){
		Map<String,List<AnalyseUnit>> map=new HashMap<String,List<AnalyseUnit>>();
		Iterator<Tuple> it = id_domain_rate.iterator();
		while(it.hasNext()){
			try {
				Tuple Logs = it.next();
				List<Object> obj=Logs.getAll();
				int obj_index=totalIndex+1;
				int totalNumber=Integer.parseInt(obj.get(totalIndex).toString());
				for(;obj_index<obj.size();obj_index+=2){
					String temp_data = obj.get(obj_index).toString();
					String other_data = obj.get(obj_index+1).toString();
					float rate=Float.parseFloat(other_data.substring(0, other_data.indexOf("%")));
					if(rate>=30&&totalNumber>7000){
						List<AnalyseUnit> list=new ArrayList<AnalyseUnit>();
						if(map.containsKey(temp_data)){
							list=map.get(temp_data);
						}
						AnalyseUnit au=new AnalyseUnit();
						au.setId(obj.get(totalIndex-1).toString());
						au.setTotalNum(totalNumber);
						au.setImpNum((int)(totalNumber*rate*0.01));
						au.setRate((float)(rate*0.01));
						list.add(au);
						map.put(temp_data, list);
						break;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return map;
	}
	private DataBag map2DataBag(Map<String,List<AnalyseUnit>> possibleCheatDomain){
		List<Object> list=new ArrayList<Object>();
		DataBag output = BAG_FACTORY.newDefaultBag();
		for(Map.Entry<String, List<AnalyseUnit>> entry : possibleCheatDomain.entrySet()){
			for(AnalyseUnit au : entry.getValue()){
				list.clear();
				list.add(entry.getKey());
				list.add(au.getId());
				list.add(au.getTotalNum());
				list.add(au.getImpNum());
				list.add(au.getRate());
				output.add(TUPLE_FACTORY.newTuple(list));
			}
		}
		return output;
	}
}
