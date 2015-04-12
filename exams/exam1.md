# Exam 1

This exam is consisted of two parts.  You must work on the exam alone -- do not discuss the exam with anyone as the consequence of failure to comply will be pretty severe.  You have until 11:59 PM Sunday 12 April 2015 to complete the exam.

## Discussion Part

Answer each of the following questions carefully.  It is not enough to "quote" the text.  Note that some questions require an example for full credit.

1. What is _Data Gravity_ according to the article *Cognitive Augmentation*?  Provide an example of _Data Gravity_.

2. What are some of the [impetuses](http://www.merriam-webster.com/dictionary/impetus) behind the proliferation of the _Graph Databases_ technologies.

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

You only need the default python install version 2.7.x.  Do not use python 3.x 

> Are we going to upload at csns or we are going to mail you?

You will upload `answer.txt` and `data_duck.py` to CSNS.  Note that the file names as specified are `answer.txt` and `data_duck.py` (all lowercase).

> Is this "Sunday 08:00 AM to 9:10 AM" under logistics section on URL mentioned considered date?

I would say that you have one `Day of the week` and two `Time of day`

> For each of the date occurences on that page, we have to report in following format ?

```
Seasons: 4 
Year: 12
Month: 2
Day of the Week: 2
Time of day: 2
Date: 12
```

The number after the colon `:` denotes how many of the occurrences of that data is discovered in the document.  In the above example, we witness four references to `Seasons` in a document, twelve references to `Year`, two references `Day of the week`, two references to `Time of day`, and twelve references to `Date`.

> Are we restricted to using Python's standard modules or can we use any python library? To be more specific, can I use Requests (http://docs.python-requests.org/en/latest/) and Beautiful Soup (http://www.crummy.com/software/BeautifulSoup/) libraries.

No you should use the standard python distribution.  Do not use anything exotic for this assignment.  For your homework and project, you have more options.

> Do you mean rather some measure of time, per the categories you mention in #3 ?  A date is simply 00/00/0000.

The `Date` category is when you detect `x/x/xxxx`.  Note that `x` can be one or two digits (for day and month).  `xxxx` must be four digits for year.  So `2/2/15`, according to the specification, is not a valid date and should be ignored.  However, I will accept it if it makes your coding easier.

> In other words, the program should search for instances of text strings that represent (a) any one of the 7 days of the week, any one of the 4 seasons, any time expression, etc.

It should detect all six possible date configuration as specified and tally up the count for each of the categories.

> Would the assignment be exactly the same were you to leave out #2 ?  How is #2 not a subset of #3?  

It is possible to double count a detection for `Year` and `Date`.  For example, `2/2/2015` will increase the `Year` counter by one *and* the `Date` counter by one.  However, `2/2/1999` will only increase the `Date` counter.

> I'm stuck at Install pip. How to proceed ahead?

You should not need to install `pip` only use the default python installation.  Also you are on your own with python installation.  FWIW, Windows users should only use the 2.7.9 installation from `https://www.python.org/downloads/` and Mac users no need to install anything.  Linux users, you might need to install 2.7.9 via `yum` or `aptitude` -- in most cases, Linux systems include python as part of the default install.

> Does this "CS594-spring-2015​" is considered a valid Year 2015 and valid season Spring?

I see `spring` (case insensitive) and I also see `2015`.  What do you see?
