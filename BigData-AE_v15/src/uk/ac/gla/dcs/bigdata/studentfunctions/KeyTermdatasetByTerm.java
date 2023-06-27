package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.TermOccurenceCount;

public class KeyTermdatasetByTerm implements MapFunction<TermOccurenceCount, String> {

	private static final long serialVersionUID = 2376068827959306566L;
	//returns term to be used as key for dataset
	@Override
	public String call(TermOccurenceCount value) throws Exception {
		// TODO Auto-generated method stub
		return value.getTerm();
	}
}
