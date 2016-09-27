package cheat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * 传入某媒体的作弊数据，分析并且返回该媒体的权重，作弊原因
 * @author paiyue
 *
 */
public class MultiCheatWeight extends EvalFunc<Tuple>{
	@Override
	public Tuple exec(Tuple input) throws IOException{
		if(input==null||input.size()==0){
			return null;
		}
		Object obj=(Object)input.get(0);
		if(!(obj instanceof DataBag)){
			int errCode = 2114;
			String msg="Expected input a DataBag,but got"+obj.getClass().getName();
			throw new ExecException(msg,errCode,PigException.BUG);
		}
		//某个媒体一周内的所有作弊行为，权重
		DataBag db=(DataBag)obj;
		Iterator<Tuple> iter = db.iterator();
		//保存多个作弊行为的变量
		Set<String> cheatrule = new HashSet<String>();
//		int maxdata=0;//流量/点击数
		int weight=0;//权重
		while(iter.hasNext()){
			Tuple tuple=iter.next();
			List<Object> list=tuple.getAll();
			if(list.get(3).toString()!=null){
				cheatrule.add(list.get(4).toString());
			}
			
			int tempweight=Integer.parseInt(list.get(3).toString());
			//找权重最大的那个作弊数据
			if(tempweight>=weight){
				weight=tempweight;
//				int data=Integer.parseInt(list.get(1).toString());
//				if(data>maxdata){
//					maxdata=data;
//				}
			}
		}
		//若该媒体有多个作弊行为，说明作弊严重
		if(cheatrule.size()>1){
			weight=100;
//			maxdata=0;
		}
		TupleFactory tuplefactory = TupleFactory.getInstance();
		List<Object> list=new ArrayList<Object>();
		list.add(weight);
		list.add(cheatrule.toString().replace("[", "").replace("]", ""));
		return tuplefactory.newTuple(list);
	}
}
