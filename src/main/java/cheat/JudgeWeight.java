package cheat;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class JudgeWeight extends EvalFunc<String> {
	@Override
	public String exec(Tuple input) throws IOException{
		if(input==null||input.size()==0){
			return null;
		}
		Object cheatdata = (Object)input.get(0);
		if(!(cheatdata instanceof DataBag)){
			int errCode = 2114;
            String msg = "Expected input to be databag, but" +
            " got " + cheatdata.getClass().getName();
            throw new ExecException(msg, errCode, PigException.BUG); 
		}
		DataBag cheat_bag = (DataBag)cheatdata;
		return this.getWeight(cheat_bag);
	}
	//根据数据，得到媒体和对应权重
	private String getWeight(DataBag cheat_bag){
		Iterator<Tuple> it = cheat_bag.iterator();
		float cheat_rate=0;
		String domain="";
		while(it.hasNext()){
			//1.一天多个cookie流量2.一个cookie流量过多
			Tuple tuple=it.next();
			List<Object> list=tuple.getAll();
			if(list.size()==0){
				continue;
			}
			domain=list.get(0).toString();
			String strrate=list.get(4).toString();
			float rate=Float.parseFloat(strrate.substring(0, strrate.indexOf("%")));
			if(cheat_rate<rate){//选取最高的比例
				cheat_rate=rate;
			}
		}
		//根据流量占比设置作弊权重
		int weight=0;
		if(cheat_rate<=50){
			weight=2;//一般作弊，权重2
		}else{
			if(cheat_rate>50&&cheat_rate<=70){
				weight=15;//较严重作弊
			}else{
				if(cheat_rate>70){
					weight=100;//严重作弊
				}
			}
		}
		return domain+"\t"+weight;
	}
}
