"""
We are interested seeing what are the top tags used in posts.

Write a mapreduce program that would output Top 10 tags, ordered by the number of questions they appear in.

Expected results can be found here: https://www.udacity.com/wiki/ud617/local-testing-instructions/top-tags

To execute, run the following command

    cat student_test_posts.csv | top_tags_mapper.py | sort | top_tags_reducer.py
"""

import sys


def reducer():
    old_key = None
    total_count = 0
    unsorted_result = ''

    for line in sys.stdin:
        data_mapped = line.strip().split('\t')
        if len(data_mapped) != 2:
            continue
        this_key, value = data_mapped

        if old_key is not None and old_key != this_key:
            unsorted_result += "{}\t{}:".format(old_key, total_count)
            old_key = this_key
            total_count = 0

        total_count += int(value)
        old_key = this_key

    if old_key is not None:
        unsorted_result += "{}\t{}".format(old_key, total_count)

    sorted_results = sorted(unsorted_result.split(':'),
                            key=lambda res: res.split('\t')[1],
                            reverse=True)[0:10]

    for result in sorted_results:
        print(result)


reducer()
