package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.TermOccurenceCount;

public class KeyNewsByID implements MapFunction<TermOccurenceCount, String> {

	private static final long serialVersionUID = 525739182048149914L;
	//returns each article id to be used as key
	@Override
	public String call(TermOccurenceCount value) throws Exception {
		return value.getNewsID();
	}
}