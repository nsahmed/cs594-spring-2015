
# Exam 2

This exam is consisted of only one part.  You must work on the exam alone.

1. Given the following JSON string in `data.json`:

```
Crime: {
  murder: 4,
  robbery: 8,
  grand-theft: {
    auto: 12,
    jewelery: 11
  }
  buglary: 9
}
```

The python code to read in `data.json` and print a value;

```
import json

with open('data.json') as data_file:    
    data = json.load(data_file)

print ____
```

Write the missing python code to print out the total `grand-theft` crime.  The output should say `grand-theft: 23`.

2. With regards to financial market, which one of the following is an example of "ambient data"?  (a) daily average price of a stock, (b) daily total volume (number of shares exchanged) of a stock, (c) news relating to the market segment of the stock, (d) news relating to a stock, (e) news relating to the majority shareholders (owners) of the stock.

3. 3rd order of knowledge is related to which of the following data science application? (a) comprehension, (b) detection, (c) prediction, (d) visualization

4. According the video `Awesome Big Data Algorithms` what is the main cost of the Bloom Filters? (a) String, (b) NumPy, (c) Hash Table, (d) Tree

5. In one or two sentences describe what is the purpose of the Bloom Filters?


