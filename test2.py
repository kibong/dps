from __future__ import print_function
from pyspark import SparkContext
import sys
if __name__ == "__main__":
    
    sc = SparkContext(appName="WordCount")
    text_file = sc.textFile('s3://team-model-data-preprocess/test/dataset/test_korean_jsonl_data/0/text_0.jsonl')

    import pdb
    pdb.set_trace()
    counts = text_file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    counts.saveAsTextFile('s3://team-model-data-preprocess/test/output/test2')
    sc.stop()