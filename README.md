# Big Data Purification 

*A python script for big data purification of a sqlite3 database.*

This repository contains code that cleans big data -
company names from the sqlite3 database
semos_company_names.db.

## Description
* The class CleanCompanyNames from the script
`sqlite3_with_pandas.py` presents the big data purification
in three ways - with the multiprocessing library,
with the threading library, and without these two libraries. 
It also shows the time needed to complete each step of 
the purification, as well as the total time.

## Contact
Author
* Kristina Jovanovska (kristina.jovanovska@protonmail.com)

## Table of Contents 
* Requirements
* Database 
* How to use this code 
* Use your own database

## Requirements
For this script you need the following libraries and modules:

1. sqlite3 ([documentation](https://docs.python.org/3/library/sqlite3.html))
2. threading ([documentation](https://docs.python.org/3/library/threading.html))
3. time ([documentation](https://docs.python.org/3/library/time.html))
4. pandas ([documentation](https://pandas.pydata.org/docs/user_guide/index.html) \, [installation](https://pandas.pydata.org/docs/getting_started/install.html))
5. multiprocessing ([documentation](https://docs.python.org/3/library/multiprocessing.html) \,  [pip_install](https://pypi.org/project/multiprocessing/))



## Database
The sqlite3 database semos_company_names.db
has one table - companies, 3 columns
(id, name, company_name_cleaned) and 20 000 rows.

## How to use this code 
1. To purify the big data \
   \
     1.1. without the multiprocessing library - in `sqlite3_with_pandas.py` 
     uncomment only the following lines 
    ```
     ccn = CleanCompanyNames()
     ccn.run_program()
     ``` 

    1.2. with the multiprocessing library - in `sqlite3_with_pandas.py`
    uncomment only the following lines
    ```
     ccn = CleanCompanyNames()
     ccn.with_multiprocessing()
    ```

    1.3. with the multiprocessing library - in `sqlite3_with_pandas.py`
     uncomment only the following lines    
    ```
     ccn = CleanCompanyNames()
     ccn.with_threading()
    ```
2. Print rows from the sqlite3 database - in `sqlite3_with_pandas.py`
     uncomment only the following lines    
    ```
     ccn = CleanCompanyNames()
     ccn.print_rows()
    ```

## Use your own database
1. Change the database name, table name and column names
to your own sqlite3 database name, table name and column names.
2. In the method pd.read_sql() change the chunksize 
appropriate to the number of rows in your table. 
3. Use the replace() method for any additional occurrences
of substrings that need to be replaced.