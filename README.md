# README #

In this project, the Big Data Processing tool, **Apache Spark**, is used to answer three queries on a [covid-19 dataset](https://www.kaggle.com/imdevskp/corona-virus-report?select=full_grouped.csv).   
Queries' output will be stored onto the well-known document No-SQL datastore, **MongoDB**. 

### Most Relevant APIs ###

* **PySpark**  
* **PyMongo**


### How to Setup ###

    pip3 install -r requirements.txt

### How to Run ###

    python3 QueryTool.py -q [queryId] -m [month] -d [date] -c [country] -S [show dataset details] -h [help]

*	-q: it's the query identifier.
*	-m: it allows to specify the month.
*	-d: it allows to specify the date.
*	-c: it allows to specify the country.
*	-S: it shows dataset details.
*	-h: it prompts help menu.

### Supported Queries ###

#### Query #0 ####
For a given month (user input), for each country compute new cases's average in that month.

#### Query #1 ####
This query, for a given month (user input), for each country and for each day, compute:

* The fraction of new deaths in the next week and in the next two weeks.
* New cases in that day.

#### Query #2 ####
This query, for a given country (user input), until a date (user input), compute the fraction between cumulative recovered and new cases. 