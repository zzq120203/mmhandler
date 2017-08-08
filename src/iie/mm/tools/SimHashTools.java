package iie.mm.tools;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;


public class SimHashTools {

	public static long genSimHashCode(String content) {
		List<Term> list=ToAnalysis.parse(content).getTerms();
		int []midBitValue=new int[64];
		Map<String,Integer> map=new HashMap<String,Integer>();
		for(int i=0;i<list.size();++i){
//			System.out.println(list.get(i).getName());
			if(map.containsKey(list.get(i).getName())){
				map.put(list.get(i).getName(), map.get(list.get(i).getName())+1);
			}else{
				map.put(list.get(i).getName(), 1);
			}
		}
		
		for(Map.Entry<String, Integer> en:map.entrySet()){
			long hashV=MurmurHash.hash64(en.getKey());
			for(int i=0;i<63;++i){
				if((hashV&(1L<<i))==0){
					midBitValue[i]-=1;
				}else{
					midBitValue[i]+=1;
				}
			}
		}
		
		long hashV=0L;
		for(int i=0;i<64;++i){
			if(midBitValue[i]>0){
				hashV=hashV|(1L<<i);
			}
		}
//		System.out.println(content+":"+hashV);
		return hashV;
	}
}
