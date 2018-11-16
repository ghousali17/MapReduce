# Hadoop Introduction

## Components
### 1. Word Count
Returns all words and their number of occurances in a text file.
### 2. Word Length Count
Returns all distinct lengths of words and their number of occurances in a text file.
### 3. Bigram Initial Count
Returns the initials of all consecutive words the number of times they occur together in a text file.
### 4. Bigram Initial Relative Frequency
Returns the initials of all consecutive words and the frequency of their occurance in a text file relative to the count of the prefix.
```
f(B|A) = count(A, B) / count(A)
```
## Usage
1. Create hdfs directory /input and put input text file into this directory
2. Execute one of the run* scripts to run the respective preogram.

