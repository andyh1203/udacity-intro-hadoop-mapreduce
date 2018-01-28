"""
We are interested seeing what are the top tags used in posts.

Write a mapreduce program that would output Top 10 tags, ordered by the number of questions they appear in.

Expected results can be found here: https://www.udacity.com/wiki/ud617/local-testing-instructions/top-tags

To execute, run the following command

    cat student_test_posts.csv | top_tags_mapper.py | sort | top_tags_reducer.py
"""

import sys
import csv


def mapper():
    reader = csv.reader(sys.stdin, delimiter='\t', quotechar='"')
    next(reader, None)
    for line in reader:
        node_type = line[5]
        if node_type == 'question':
            tag_names = line[2]
            tags = tag_names.split()
            for tag in tags:
                print('{}\t{}'.format(tag, 1))


mapper()
