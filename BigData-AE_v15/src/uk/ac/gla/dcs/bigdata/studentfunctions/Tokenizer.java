package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenizedNewsMap;

public class Tokenizer implements FlatMapFunction<NewsArticle, TokenizedNewsMap> {

	private static final long serialVersionUID = 2372004374121696720L;
	LongAccumulator wordCountAccumulator;
	LongAccumulator numberofdocsAccumulator;
	LongAccumulator totalwordCountAccumulator;

	public Tokenizer(LongAccumulator wordCountAccumulator, LongAccumulator numberofdocsAccumulator,
			LongAccumulator totalwordCountAccumulator) {
		this.wordCountAccumulator = wordCountAccumulator;
		this.numberofdocsAccumulator = numberofdocsAccumulator;
		this.totalwordCountAccumulator = totalwordCountAccumulator;
	}

	public Iterator<TokenizedNewsMap> call(NewsArticle news) throws Exception {

		// resets word count for the article
		wordCountAccumulator.reset();
		numberofdocsAccumulator.add(1);
		//Initializes our overriden processor method to take in accumulator values
		TextPreProcessor processor = new TextPreProcessor(wordCountAccumulator, totalwordCountAccumulator);
		List<String> TitleTokenized = processor.process(news.getTitle());
		
		//if tokenized title is empty, return empty since we dont want this row in our dataset
		if (TitleTokenized.isEmpty()) {

			List<TokenizedNewsMap> newsList = new ArrayList<TokenizedNewsMap>(0);
			return newsList.iterator();
		}

		List<String> contentTokenized = new ArrayList<>();
		
		List<ContentItem> contents = news.getContents();
		int paragraphCounter = 0;
		
		//apends tokenized content while subtype is paragraph and only for the first five paragraphs.	
			for (int i = 0; i < news.getContents().size(); i++) {

				if (contents.get(i) == null) {
					continue;
				}
				if (contents.get(i).getSubtype() == null) {
					continue;
				}

				if (contents.get(i).getSubtype().equalsIgnoreCase("paragraph") && paragraphCounter < 5) {

					contentTokenized.addAll(processor.processCount(contents.get(i).getContent()));

					paragraphCounter++;
				}
			}
			

		
		//creates new tokenizedNewsMap object and sets values
		TokenizedNewsMap Tnews = new TokenizedNewsMap();
		Tnews.setId(news.getId());
		Tnews.setTitle(news.getTitle());
		Tnews.setArticle(news);
		Tnews.setWordCount(wordCountAccumulator);
		
		//if the content is empty, returns empty
		if (contentTokenized.isEmpty()) {

			List<TokenizedNewsMap> newsList = new ArrayList<TokenizedNewsMap>(0);
			return newsList.iterator();
			
		//combines tokenized title and content together. Adds content to TokenizedNewsMap object and returns it
		} else {

			contentTokenized.addAll(TitleTokenized);
			Tnews.setContentTokenized(contentTokenized);
			List<TokenizedNewsMap> newsList = new ArrayList<TokenizedNewsMap>(1);
			newsList.add(Tnews);
			return newsList.iterator();
		}
	}
}
