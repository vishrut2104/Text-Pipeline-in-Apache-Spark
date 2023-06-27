package uk.ac.gla.dcs.bigdata.studentstructures;

import java.util.List;

import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class TokenizedNewsMap {

	List<String> contentTokenized; // the contents of the article body
	String title; 
	String id; 
	long wordCount;
	int docOccurence = 0;
	int totalOccurence;
	NewsArticle article;

	public TokenizedNewsMap() {};
	public TokenizedNewsMap( String title,List<String> contentTokenized, NewsArticle article) {
		super();
		this.title=title;
		this.contentTokenized = contentTokenized;
		this.article = article;
	}
	
	
	public NewsArticle getArticle() {
		return article;
	}
	public void setArticle(NewsArticle article) {
		this.article = article;
	}
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
	public List<String> getContentTokenized() {
		return contentTokenized;
	}

	public void setContentTokenized(List<String> contentTokenized) {
		this.contentTokenized = contentTokenized;
	}

	public void setTitle(String title) {
		this.title = title;
	}


	public String getTitle() {
		return title;
	}
	
	
	public long getWordCount() {
		return wordCount;
	}

	public void setWordCount(LongAccumulator wordCount) {
		this.wordCount = wordCount.value();
	}
}
