"""
We are interested to see if there is a correlation between the length of a post and the length of answers.

Write a mapreduce program that would process the forum_node data and output the length of the post and the
average answer (just answer, not comment) length for each post. You will have to decide how to write both
the mapper and the reducer to get the required result.

Expected results can be found here: https://www.udacity.com/wiki/ud617/local-testing-instructions/average-length

To execute, run the following command

    cat student_test_posts.csv | post_and_answer_length_mapper.py | sort | post_and_answer_length_reducer.py

"""

import sys
import csv


def mapper():
    reader = csv.reader(sys.stdin, delimiter='\t', quotechar='"')
    next(reader, None)
    for line in reader:
        node_type = line[5]
        node_id = line[0]
        body = line[4]
        print("{}\t{}\t{}".format(node_id, node_type, body.replace('\n', ' ')))


mapper()
