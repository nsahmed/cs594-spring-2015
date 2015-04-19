# Lecture 03

Let us recap the four steps:

1. Acquire Data
2. Store Data
3. Analyze Data
4. Visualize Data

Today we focus on the first step: `Data Acquisition`. 

## Issues dealing with data acquisition

1. Size -- is it possible to move large amount of data from a source?
2. Computation -- what is the cost to compute your data?  We are particularly interested in _O(n^k)_ what is a realistic _k_?
3. Transportation cost -- certainly this is the first challenge.  Before we can even deal with computation and storage, we have to work about cost of transportation

## Data is not always fresh

We have to think about the *continuously* getting fresh data.  An asynchronous model is required. To always acquire the latest data.  So how often do we refresh our `getter`?

## Diversity in source

We need acquire our data from multiple sources.  What are possible sources for data to acquire?

1. FTP (traditional old but reliable)
2. HTTP (screen scraping)
3. Formalized data provider (SOAP, JSON and other webservices)

Most popular method of acquiring data is off a webserver via `http` protocol using `REST` api.  The data consumed and data produced is usually (these days) in `JSON` format.
