
# Exam 4

Please note that this is an individual effort exam.  Students are required to adhere to the course policy on academic integrity.

You have until 5 PM Sunday to complete this exam.  Please email the instructor with any questions.  Under no circumstancesare you permitted to discuss this exam with anyone else.

---

We will use the stop words found here `http://xpo6.com/list-of-english-stop-words/` for questions 1 - 4 below.  Please down the raw txt file from: `http://xpo6.com/wp-content/uploads/2015/01/stop-word-list.txt` which is at the bottom of the page.

Consider [The Picture of Dorian Gray](https://www.gutenberg.org/cache/epub/174/pg174.txt) by Oscar Wilde, answer questions 1 and 2.

1. Determine the most frequently occuring word in [The Picture of Dorian Gray](https://www.gutenberg.org/cache/epub/174/pg174.txt) that is **not** a stop word.

2. Do a histogram of the 100 most frequently occuring words in [The Picture of Dorian Gray](https://www.gutenberg.org/cache/epub/174/pg174.txt).  Your histogram should ignore stop words.  Be sure to annotate the plot.

Consider Mark Twain's classic works [The Adventures of Huckleberry Finn](https://www.gutenberg.org/cache/epub/76/pg76.txt) and [The Adventures of Tom Sawyer](https://www.gutenberg.org/cache/epub/74/pg74.txt), answer questions 3 and 4.

3. Determine top 10 words that are common in both text and are not stop words.

4. Which of the two works has more unique words?

### This question will give me a good idea on how much you understand your project and what is your contribution level.

5. What is your personal contribution to the Data Science class's project? What is the most interesting finding your team uncovered as a result of your Data Science project?

---

## Frequently Asked Questions (FAQ)

> What is a `unique word`?

A `unique word` is a word that occurs only once in the text. For example, body of text is `the green dog is bigger than the blue dog`, the unique words are: `green`, `is`, `than`, and `blue`.  However, given that `is` and `than` are `stop words`, therefore we do not count them.  So the unique words are really: `green` and `blue`.

> I've question about ' apostrophe, should i count it as special character?
if i count as special character then dorian will occur 420 time but if i don't then it will make dorian - 415 times and dorian's - 5 times.  That will make two separate words.  And it will also break don't as don.

Separate them into multiple words.

> The page for the stopwords has 3 different lists, and there are a few errors in each.

Please use `http://xpo6.com/wp-content/uploads/2015/01/stop-word-list.txt` as the official stop word list for this exam.  Note everything is in lowercase.  Hint: your text should also be in lowercase.

> It seems that there is a daily rate limit for the online text file which we have to use for exam4. So after a certain limit the IP address gets blocked for 24hrs. So is it ok to copy the contents of the book in a text file and read the text file and do further processing?

You can assume that the file has been downloaded to local disk.  In fact, your program should read it from local disk.  The expected three text files should be in the same directory as the python code with the names: `pg174.txt`, `pg76.txt`, and `pg74.txt`. 

> For exam 4 do we have to submit our code? or only the answer? 

Please submit the following files: `most_frequent_word.py`, `histogram.py`, `common_words.py`, `unique_words.py`, `histogram.png`, and `answer.txt`. The format of your `answer.txt` should be:

```
most_frequent_word.py: homeless
common_words.py: john professor ... is awesome etc
unique_words.py: Tom Sawyer
``` 

> I was wanting to use PyEnchant to do some word processing for Exam4.  Would it be ok to use this library?

No do not use anything other than the default Python library.  You do not need to.  Also, make sure that your code is compatible with python 2.7.x.  Do not use python 3.x -- you will not receive credit if your code does not run.  This is not negotiable.

> Which libraries of python can we use?

The default python library. No external libraries.

> Can we use an external library?

No!

> What external libraries other than the default 2.7.x library can we use?

None

> Is it ok if we use so and so library that is not included with the 2.7.x library?

No

> Can we make our code only works with python 3.x and not 2.7.x?

No

> Should we ensure that our code works with 2.7.x?

Yes

> My code does not work with python 2.7.x but it works with python 3.x.  Should I worry?

Yes

> My friends all use python 3.x, should I do the same?

No


