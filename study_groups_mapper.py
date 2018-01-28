"""
We might want to help students form study groups. But first we want to see if there
are already students on forums that communicate a lot between themselves.

As the first step for this analysis we have been tasked with writing a mapreduce
program that for each forum thread (that is a question node with all it's answers
and comments) would give us a list of students that have posted there - either asked
the question, answered a question or added a comment. If a student posted to that
thread several times, they should be added to that list several times as well,
to indicate intensity of communication.

Expected results can be found here: https://www.udacity.com/wiki/ud617/local-testing-instructions/study-groups

To execute, run the following command

    cat student_test_posts.csv | study_groups_mapper.py | sort | study_groups_reducer.py

"""

import sys
import csv


def mapper():
    reader = csv.reader(sys.stdin, delimiter='\t', quotechar='"')
    next(reader, None)
    for line in reader:
        node_type = line[5]
        node_id = line[0]
        author_id = line[3]
        print("{}\t{}\t{}".format(node_id, node_type, author_id))


mapper()
