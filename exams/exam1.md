# Exam 1

This exam is consisted of two parts.  You must work on the exam alone -- do not discuss the exam with anyone as the consequence of failure to comply will be pretty severe.  You have until 11:59 PM Sunday 12 April 2015 to complete the exam.

## Discussion Part

Answer each of the following questions carefully.  It is not enough to "quote" the text.  Note that some questions require an example for full credit.

1. What is _Data Gravity_ according to the article *Cognitive Augmentation*?  Provide an example of _Data Gravity_.

2. What are some of the impetuses for the proliferation of the _Graph Databases_ technologies.

3. According to Ben Lorica, the article "Streamlining Feature Engineering" (p. 53) references the [Data Science Pipeline](http://radar.oreilly.com/2013/09/data-analysis-just-one-component-of-the-data-science-workflow.html) with the final step "Story-Telling".  How does that map to our data science approach?  In particular, focus on the last part: Visualization.

4. What is an argument that one might make *against* the building data science solution through the combination of tools?  Provide a concrete example.

Answer the following multiple choice questions.  Please refer to the syllabus if you have questions.

4. Regarding the course policy on academic integrity, which of the following action is considered cheating:
(a) Giving an answer on an exam or homework assignment to another student
(b) Receiving an answer on an exam or homework assignment from another student
(c) Searching the Internet for an answer on an exam or homework assignment and not citing the source of information
(d) All of the above

5. If caught cheating, which of the following is a consequence specific to this course?
(a) Reminder not to do it again
(b) Receiving an F on the assignment/exam
(c) Receiving an F for the course
(d) Nothing, the student will be congratulated for his/her stealthy work

6. I did not cheat on this quiz
(a) true
(b) false

## Programming Part

Consinder the following URL:

```
https://raw.githubusercontent.com/csula/cs594-spring-2015/master/Syllabus.md
```

Write a python program that (1) reads the contents of the above URL, (2) detect the presence of dates, and (3) count the number of events for each of the following categories:

* Seasons (Spring, Summer, Fall, Winter)
* Year (4 digit number between 2000 and 2015)
* Month (January, February, ..., December)
* Day of the week (Monday, Tuesday, ..., Sunday)
* Time of day (xx:xx am or xx:xx pm)
* Date (x/x/xxxx)

Your code will detect any possible date references and it should ignore cases. Here is an example of a report: 

```
Seasons: 4 
Year: 12
Month: 2
Day of the Week: 2
Time of day: 2
Date: 12
```

Submit two files to CSNS: `answer.txt` and `data_duck.py`.  You may hardcode the URL into the python code.

---

Below are answers to students' questions on the exam.  Check back often as I will be updating this page throughout the day:

> As mentioned, we also need to submit .py file. So, is there any specific application we need to install to code data_duck.py or we can use any available software? 

You only need the default python install version 2.7.3. Do not use python 3.x 


