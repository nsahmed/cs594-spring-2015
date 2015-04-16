# Lecture 02

Today we had our first quiz.  The purpose of the quiz was to see if you really wanted to take this data science course.  Here, I will sketch out the solution and certaintly we will go over it in class.

## Discussion Part

1. Data Gravity suggests that data is simply too large to move (or does it really make sense to move the data?).  If so, let us bring the computation to the data.  For example, instead of downloading a huge data set from Amazon EC2 cloud, maybe we should keep the data processing there...in the cloud.

2. Graph Daabases are databases that focus on relationnship.  The biggest motivation is the so-called social networking evolution.  So conceivably one can ask complex questions like how is this related to that by way of "connections" between this and that.

3. Story telling is essentially what we do with our final step.  Which is to visualize our data analytics.

4. Complexity and interopability are my two choices.  Of course, there are others.

## Programming Part

There are many ways to attack this problem.  However using REGEX is your best option.  The general algorithm works as followed:

1. Download your data with url library
2. Construct a set of REGEX rules (six to be specific)
3. Apply each regex rule & count on the data buffer


