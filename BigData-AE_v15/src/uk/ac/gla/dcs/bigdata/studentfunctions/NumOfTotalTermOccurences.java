package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Iterator;

import org.apache.spark.api.java.function.MapGroupsFunction;
import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.studentstructures.TermOccurenceCount;

public class NumOfTotalTermOccurences implements MapGroupsFunction< String, TermOccurenceCount , Tuple2<String,Integer>>{
	
	private static final long serialVersionUID = -9152578766831227471L;
	//calculates total number of term occurences for each term key by totalling and returns tuple
	@Override
	public Tuple2<String,Integer> call(String key, Iterator<TermOccurenceCount> values) throws Exception {
		int sum = 0;
		while (values.hasNext()) {
			TermOccurenceCount term = values.next();
			sum =  sum +  term.getNumOfOccurences();
		}
		Tuple2<String,Integer> termCount = new Tuple2(key,sum);
		return termCount;
	}
}
