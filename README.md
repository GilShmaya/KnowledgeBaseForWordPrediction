# KnowledgeBaseForWordPrediction

###### Submitters :

Gil Shmaya 209327303

Yael Elad 318917622


##### Summary :
In this Assignment we generated a knowledge-base for English word-prediction system, based on Google 3-Gram English dataset, using Amazon Elastic Map-Reduce. 
The system calculates the probability of each trigram (w1,w2,w3) founded in the given corpus, and generates the output knowledge base with the resulted probabilities.


###### How to run the project :

1. update the credential - on the local dir /.aws/credentials

2. create the following buckets : "s3n://assignment2gy"

*** 

3. Upload eng-stopwords.txt to the bucket you created and make sure you name it "eng-stopwords.txt".

3. Upload the following jars to the "assignment2gy" bucket:
    - Splitter.jar
    - NrTrCalculator.jar
    - Joiner.jar
    - DEprobability.jar
    - SortOutput.jar

4. run java -jar MainLogic/target/MainLogic-1.0-SNAPSHOT.jar



#### Map Reduce Jobs :

##### Job 1 - Splitter 

###### Mapper :

- Gets as input lines from the English 3-Gram dataset of google 

* Each line contains the following fields:

3-gram 
year - The year for this aggregation
occurrences - The number of times this 3-gram appeared in this year
pages - The number of pages this 3-gram appeared on in this year
books - The number of books this 3-gram appeared in during this year

- Map every line into <3-gram, Occurrences>, the Occurrences include a division of the corpus into two parts and the occurrences of every 3-gram.

###### Combiner : 

- Performing a local aggregation on the mapper's output : Combine the 3-grams Occurrences the mapper created in each server

- Generates two outputs for each 3-gram :

* <3-gram , new Occurrences(true, r1)> 

* <3-gram , new Occurrences(false, r2)> 

*** By using the Combiner we reduce the amount of data that needs to be transmitted over the network and processed by the reducer .

###### Reducer :

- Calculating the parameter N (indicates the total number of 3-grams in the corpus)

- For each 3-gram, calculate its total occurances in each part of the splitted corpus (r1, r2) 

- Generates one output for each 3-gram : <3-gram, r1, r2>



##### Job 2 - NrTrCalculate 
 
###### Mapper :

- Gets as input the files from job1 (<3-gram, r1 r2>)

- Map each line into two files :

<r1, Aggregator> : Aggregator contains the data: corpus group 1, parameter R, and r2. When R is equal to r1.

<r2, Aggregator> : Aggregator contains the data: corpus group 2, parameter R, and r1. When R is equal to r1.

###### Reducer :

- Gets as input the files from the mappersand combines the values of Nr1, Nr2, Tr1, Tr2 for each R. When R is a number of occurrences in the corpus.



##### Job 3 - Joiner 

###### Mapper :

- Gets the output files of job1 and job2 as input: 

* <3-gram> <r1> <r2>  - from job1 

* <R> <Nr> <Tr> (for each corpus) - from job2

- For each line check if it came from job1 or job2 :

* In case the line came from Job1 (Splitter) :

- Generates the output - <R, (corpus_group, 3-gram)>
 
* In case the line came from Job2 (NrTrCalculator):

- Generates the output <R, (corpus, Nr, Tr)>

###### Reducer :

- Generates two output files for each 3-gram by joining the data arrived from job1 and job2 :

* <trigram, Nr1, Tr1> 

* <trigram, Nr2, Tr2>


 
##### Job 4 - DEprobability

###### Mapper :

- Gets as input the output files from job3 (<trigram, Nri, Tri> (i=1,2)) and the parameter N from job1.

- Calcualte the Deleted Estimation Probability for each 3-gram according to the given formula.

- Generates an output for each 3-gram with its probability - <3-gram, Probability>

###### Reducer :

- Gets a 3-gram as Key and the trigram's Nr1, Tr1, Nr2, Tr2 values as Value.

- Calculate the probability for the given 3-gram according to the Deleted Estimation method.

 

##### Job 5 - SortOutput 

###### Mapper :

- Gets as input the output files from job4, which represents the final probability for each 3-gram.

- Map every input line (w1 w2 w3 probability) into <NewProbability, w3>

* 'NewProbability' update the probability of the pair w1w2 to be a temporary probability of the 3-gram w1w2w3, in order to know which w3 should appear first with w1w2.

###### Reducer :

- Gets the pairs <NewProbability, w3> from the Mapper and creates the final output -
* key:< w1w2w3 > , value:< probability of trigram >.

* Arranging the output (The 3-grams with their probability) according to the requested order: (1) by w1w2, ascending; (2) by the probability for w1w2w3, descending.
