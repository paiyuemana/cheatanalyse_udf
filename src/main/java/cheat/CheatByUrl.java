package cheat;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class CheatByUrl extends EvalFunc<String>{
	@Override
	public String exec(Tuple input) throws IOException{
		if(input==null||input.size()==0){
			return null;
		}
		Object obj=(Object)input.get(0);
		if(!(obj instanceof DataBag)){
			int errCode = 2114;
			String msg="Expected input a DataBag,but got"+obj.getClass().getName();
			throw new ExecException(msg,errCode,PigException.BUG);
		}
		DataBag data_url =null;
		data_url = (DataBag)obj;
		Iterator<Tuple> iter = data_url.iterator();
		Map<String,Integer> map=new HashMap<String,Integer>();
		while(iter.hasNext()){
			Tuple tuple=iter.next();
			List<Object> list=tuple.getAll();
			if(list.get(2)==null){
				continue;
			}
			String url=list.get(2).toString();
			int url_num=0;
			if(map.containsKey(url)){
				url_num=map.get(url);
			}
			map.put(url, url_num+1);//统计url重复的数量
		}
		//url重复占比
		int url_totalnum=0;
		int url_lg4num=0;
		for(Map.Entry<String, Integer> entry : map.entrySet()){
			if(entry.getValue()>4){
				url_lg4num=url_lg4num+entry.getValue();//重复数>4的url数
			}
			url_totalnum=url_totalnum+entry.getValue();//总数
		}
		Float rate=new BigDecimal(String.valueOf(url_lg4num)).divide(new BigDecimal(String.valueOf(url_totalnum)), 2, BigDecimal.ROUND_HALF_UP).floatValue();
		return String.valueOf(rate);
//		if((url_lg4num*1.0/url_totalnum)>0.5){//重复url占比
//			
//		}
	}
}
