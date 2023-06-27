# Text-Pipeline-in-Apache-Spark
Developed a batch-based text search and filtering pipeline in Apache Spark to rank and filter a large set of text documents ( over half a million documents ) based on user- defined queries 

• Developed a batch-based text search and filtering pipeline in Apache Spark to rank and filter a large set of text documents based on user- defined queries
• Applied text pre-processing techniques such as stop word removal and stemming to improve the quality of the search results
• Utilized mapGroupFunction and mapFunction to transform and filter input data into required key-value pairs.
• Utilized key-value groups to group similar documents together to reduce processing time.
• Used FlatMap Function to split the text documents into individual words and applied text pre-processing techniques such as stop word removal and stemming to improve search results.
• Used the DPH ranking model to score each document for each query and efficiently calculated the statistics needed to perform the ranking • Implemented a string distance function to remove unneeded redundancy and filter out near-duplicate documents in the final ranking.
