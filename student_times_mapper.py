"""
We have a lot of passionate students that bring a lot of value to forums.
Forums also sometimes need a watchful eye on them, to make sure that posts
are tagged in a way that helps to find them, that the tone on forums stays
positive, and in general - they need people who can perform some management
tasks - forum moderators. These are usually chosen from students who already
have shown that they are active and helpful forum participants.

Our students come from all around the world, so we need to know both at what
times of day the activity is the highest, and to know which of the students
are active at that time.

In this exercise your task is to find for each student what is the hour during
which the student has posted the most posts. Output from reducers should be:

    author_id    hour

For example:

    13431511\t13
    54525254141\t21

If there is a tie: there are multiple hours during which a student has posted
a maximum number of posts, please print the student-hour pairs on separate lines.
The order in which these lines appear in your output does not matter.

You can ignore the time-zone offset for all times - for example in the following
line: "2012-02-25 08:11:01.623548+00" - you can ignore the +00 offset.

In order to find the hour posted, please use the date_added field and NOT the
last_activity_at field.

Expected results can be found here: https://www.udacity.com/wiki/ud617/local-testing-instructions/student-times

To execute, run the following command

    cat student_test_posts.csv | student_times_mapper.py | sort | student_times_mapper.py

"""
import csv
import sys


def mapper():
    reader = csv.reader(sys.stdin, delimiter='\t', quotechar='"')
    next(reader, None)
    for line in reader:
        author_id = line[3]
        added_at = line[8]
        timestamp = added_at.split()[-1]
        hour = timestamp[0:2]
        print('{}\t{}'.format(author_id, hour))


mapper()
