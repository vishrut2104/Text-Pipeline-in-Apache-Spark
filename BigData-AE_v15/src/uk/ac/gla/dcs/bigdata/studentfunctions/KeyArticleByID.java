package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;


//returns article id to use as key
public class KeyArticleByID implements MapFunction<NewsArticle,String> {

	private static final long serialVersionUID = 525739182048149914L;

	@Override
	public String call(NewsArticle value) throws Exception {
		return value.getId();
	}

}