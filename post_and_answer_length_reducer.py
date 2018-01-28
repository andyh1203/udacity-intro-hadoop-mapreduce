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


def reducer():
    answer_lengths = None
    for line in sys.stdin:
        data_mapped = line.strip().split('\t')

        if len(data_mapped) != 3:
            continue

        node_id, node_type, body = data_mapped
        if node_type == 'question':
            if answer_lengths is None:
                current_node = node_id
                question_length = len(body)
                answer_lengths = []
            else:
                try:
                    avg_answer_length = float(sum(answer_lengths) / len(answer_lengths))
                except ZeroDivisionError:
                    avg_answer_length = 0
                print("{}\t{}\t{}".format(current_node, question_length,
                                          avg_answer_length))
                current_node = node_id
                question_length = len(body)
                answer_lengths = []
        if node_type == 'answer':
            answer_lengths.append(len(body))

    if node_type == 'question':
        try:
            avg_answer_length = sum(answer_lengths) / len(answer_lengths)
        except ZeroDivisionError:
            avg_answer_length = 0
        print("{}\t{}\t{}".format(current_node, question_length,
                                  avg_answer_length))


reducer()
