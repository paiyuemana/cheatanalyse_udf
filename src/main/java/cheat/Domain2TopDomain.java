package cheat;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * 将domain转换为top domain
 *
 */
public class Domain2TopDomain extends EvalFunc<String>{
	@Override
	public String exec(Tuple input) throws IOException{
		if(input==null||input.size()==0){
			return null;
		}
		Object obj=input.get(0);
		if(obj==null){
			return null;
		}
		if(!(obj instanceof String)){
			int errCode = 2114;
            String msg = "Expected input to be chararray, but" +
            " got " + obj.getClass().getName();
            throw new ExecException(msg, errCode, PigException.BUG);
		}
		String domain=(String)obj;
		return getTopDomainWithoutSubdomain(domain);
	}
	private String getTopDomainWithoutSubdomain(String url) throws MalformedURLException {

		String host = url.toLowerCase();// 此处获取值转换为小写
		Pattern pattern = Pattern.compile("[^\\.]+(\\.com\\.cn|\\.net\\.cn|\\.org\\.cn|\\.gov\\.cn|\\.com|\\.net|\\.cn|\\.org|\\.cc|\\.me|\\.tel|\\.mobi|\\.asia|\\.biz|\\.info|\\.name|\\.tv|\\.hk|\\.公司|\\.中国|\\.网络)");
		Matcher matcher = pattern.matcher(host);
		while (matcher.find()) {
			return matcher.group();
		}
		return null;
	}
	
}
