package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Map;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.TermOccurenceCount;

public class CalculateDPHPerTerm implements MapFunction<TermOccurenceCount, TermOccurenceCount> {

	private static final long serialVersionUID = -489663898622158441L;
	int currentDocumentLength;
	double averageDocumentLengthInCorpus;
	long totalDocsInCorpus;
	short termFrequencyInCurrentDocument;
	int totalTermFrequencyInCorpus;
	Map<String, Integer> docTermDict;
	
	//passes required numbers for DPH scoring
	public CalculateDPHPerTerm(double averageDocumentLengthInCorpus, long totalDocsInCorpus,
			Map<String, Integer> docTermDict) {
		super();
		this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
		this.totalDocsInCorpus = totalDocsInCorpus;
		this.docTermDict = docTermDict;
	}

	public TermOccurenceCount call(TermOccurenceCount toc) throws Exception {

		termFrequencyInCurrentDocument = (short) toc.getNumOfOccurences();
		String term = toc.getTerm();
		totalTermFrequencyInCorpus = docTermDict.get(term);
		currentDocumentLength = toc.getDocLength();
		
		//calculates DPH for each term and article row
		toc.setDPHScore(DPHScorer.getDPHScore(termFrequencyInCurrentDocument, totalTermFrequencyInCorpus,
				currentDocumentLength, averageDocumentLengthInCorpus, totalDocsInCorpus));

		return toc;
	}
}
