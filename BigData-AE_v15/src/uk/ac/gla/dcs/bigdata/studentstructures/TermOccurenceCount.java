package uk.ac.gla.dcs.bigdata.studentstructures;


import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

//New Data object to hold DPH score, occurence count, doc length, etc.

public class TermOccurenceCount {

	String newsID;
	int numOfOccurences;
	String term;
	double DPHScore;
	int docLength;
	NewsArticle article; 
	
	
	public TermOccurenceCount() {
		
	}

	public TermOccurenceCount(String newsID, int numOfOccurences, String term, int docLength) {
		super();
		this.newsID = newsID;
		this.numOfOccurences = numOfOccurences;
		this.term = term;
		this.docLength = docLength;
	}
	
	
	public NewsArticle getArticle() {
		return article;
	}

	public void setArticle(NewsArticle article) {
		this.article = article;
	}

	public int getDocLength() {
		return docLength;
	}

	public void setDocLength(int docLength) {
		this.docLength = docLength;
	}

	public double getDPHScore() {
		return DPHScore;
	}

	public void setDPHScore(double dPHScore) {
		this.DPHScore = dPHScore;
	}



	public String getNewsID() {
		return newsID;
	}

	public void setNewsID(String newsID) {
		this.newsID = newsID;
	}

	public int getNumOfOccurences() {
		return numOfOccurences;
	}

	public void setNumOfOccurences(int numOfOccurences) {
		this.numOfOccurences = numOfOccurences;
	}

	public String getTerm() {
		return term;
	}

	public void setTerm(String term) {
		this.term = term;
	}
}