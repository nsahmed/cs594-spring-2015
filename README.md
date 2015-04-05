# cs594-spring-2015

## Big Data and Visualization

Please refer to the [syllabus](https://github.com/csula/cs594-spring-2015/blob/master/Syllabus.md) for more information about the course.  

Online real time class discussion board: [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/csula/cs594-spring-2015?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

## Homework 1

Objective: define your domain.  Once your domain has been identified, your team will come up with ways to acquire the data.  The method in which you acquire data is open -- it should be noted, however, that your data acquisition approach must be thought of as a pipe-lined model.  In other words, you software will acquire data and pass it to another stage.  For example:

```
./data_collect.py | ./data_storage.py 
```

The pipe or `|` is the standard POSIX way of moving data from one stage to the next.  Note that we do not consider the pipe model to be the *only* way which we can pass data from one stage to the next.

## Homework 2

Objective: store the data that you've acquired.  A simple approach is to save data as plain text file.  This approach although simple, it does not give the data consumer the ability to do simple data processing.  So the challenge here is to hierarchically store and organize your data.  Keep in mind that the data `product` may or may not be well structured.  Furthermore, the size of data is unknown (at least at this stage).

## Homework 3

Objective: perform an analytic model on the data.  Your team will mine the data that you've acquired and come up with an interesting question.  For our part, we'd like ask a difficult second or third order question.  The real challenge here is does the data analytic support your assertion or does it provide further insight?

## Homework 4

Objective: visualize the data.  The data has been analyzed -- now tell the story.  What better way to tell the story than with a beautiful dashboard.

## Final Project

More to come...

## Big Data Recipe

More to come...
