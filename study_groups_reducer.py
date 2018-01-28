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


def reducer():

    author_ids = []
    old_node_type = None
    forum_thread_node = None
    old_node = None

    for line in sys.stdin:
        data_mapped = line.split('\t')

        if len(data_mapped) != 3:
            continue

        this_node, this_node_type, this_author_id = data_mapped

        if old_node_type is not None and old_node_type == 'question' \
        and this_node_type == 'question':
            print('{}\t{}'.format(old_node, author_ids))
            author_ids = []

        if old_node_type is not None and old_node_type == 'question' \
        and this_node_type in ('answer', 'comment'):
            forum_thread_node = old_node

        if old_node_type is not None and old_node_type in ('answer', 'comment') \
        and this_node_type == 'question':
            print('{}\t{}'.format(forum_thread_node, sorted(author_ids)))
            author_ids = []

        old_node_type = this_node_type
        old_node = this_node
        old_author_id = this_author_id
        author_ids.append(int(old_author_id.strip('\n')))

    print('{}\t{}'.format(old_node, author_ids))


reducer()
