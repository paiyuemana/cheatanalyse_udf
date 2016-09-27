package cheat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class Url2Domain extends EvalFunc<String>{
	//将url转换为域名
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
			if(!(input_obj instanceof String)){
				int errCode = 2114;
	            String msg = "Expected input to be chararray, but" +
	            " got " + input_obj.getClass().getName();
	            throw new ExecException(msg, errCode, PigException.BUG);
			}
			String domain=input_obj.toString();
			if("".equals(domain)){
				return null;
			}
			Pattern p = Pattern.compile("(?<=//|)((\\w+)(\\-\\w+)*\\.)+\\w+");
			Matcher m = p.matcher(domain);
			if (m.find()) {
				return m.group();
			}else{
				return null;
			}
		}catch(ExecException e){
			throw e;
		}
	}
}
