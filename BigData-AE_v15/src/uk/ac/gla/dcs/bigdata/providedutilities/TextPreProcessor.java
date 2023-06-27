package uk.ac.gla.dcs.bigdata.providedutilities;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.util.LongAccumulator;
import org.terrier.indexing.tokenisation.Tokeniser;
import org.terrier.terms.BaseTermPipelineAccessor;

/**
 * This class provides pre-processing for text strings based on the Terrier IR platform.
 * In particular, it provides an out-of-the-box function for performing stopword removal
 * and stemming on a text string.
 * 
 * @author Richard
 *
 */
public class TextPreProcessor {

	
	BaseTermPipelineAccessor termProcessingPipeline; // processes an individual term
	Tokeniser tokeniser; // splits a string into multiple terms
	LongAccumulator wordCountAccumulator;
	LongAccumulator totalwordCountAccumulator;
	/**
	 * Default Constructor
	 */
	public TextPreProcessor() {
		termProcessingPipeline = new BaseTermPipelineAccessor("Stopwords","PorterStemmer");
		tokeniser = Tokeniser.getTokeniser();
	}
	public TextPreProcessor(LongAccumulator wordCountAccumulator,LongAccumulator totalwordCountAccumulator) {
		this.totalwordCountAccumulator = totalwordCountAccumulator;
		this.wordCountAccumulator = wordCountAccumulator;
		termProcessingPipeline = new BaseTermPipelineAccessor("Stopwords","PorterStemmer");
		tokeniser = Tokeniser.getTokeniser();
		
	}
	
	/**
	 * Returns an array of processed terms for an input text string
	 * @param text
	 * @return
	 */
	public List<String> process(String text) {
		String[] inputTokens = tokeniser.getTokens(text);
		
		if (inputTokens==null) return new ArrayList<String>(0);
		
		List<String> outTokens = new ArrayList<String>(inputTokens.length);
		for (int i =0; i<inputTokens.length; i++) {
			String processedTerm = termProcessingPipeline.pipelineTerm(inputTokens[i]);
			if (processedTerm==null) continue;
			outTokens.add(processedTerm);
		}
		
		return outTokens;
	}
	
	// Edited the function to calculate wordCount for each token as well as the total word count.
	public List<String> processCount(String text) {
		String[] inputTokens = tokeniser.getTokens(text);
		
		if (inputTokens==null) return new ArrayList<String>(0);
		
		List<String> outTokens = new ArrayList<String>(inputTokens.length);
		for (int i =0; i<inputTokens.length; i++) {
			String processedTerm = termProcessingPipeline.pipelineTerm(inputTokens[i]);
			if (processedTerm==null) continue;
			outTokens.add(processedTerm);
			wordCountAccumulator.add(1);
			totalwordCountAccumulator.add(1);
		}
		
		return outTokens;
	}
	
}
