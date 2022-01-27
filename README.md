# Big Data Purification 

*A python script for big data purification of a sqlite3 database.*

This repository contains code that cleans big data -
company names from the sqlite3 database
semos_company_names.db.

## Description
* The class CleanCompanyNames from the script
`sqlite3_with_pandas.py` presents the big data purification
in three ways - with the multiprocessing package,
with the threading library, and without these two libraries.
The database update is done in two ways - with dataframe to sql
and with SQL update queries.
This script also shows the time needed to complete the purification.

## Contact
Author
* Kristina Jovanovska (kristina.jovanovska@protonmail.com)

## Table of Contents 
* Requirements
* Database 
* How to use this code 
* Use your own database

## Requirements
For this script you need the following libraries, modules and packages:

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
     1.1. without the multiprocessing package nor the threading library- in `sqlite3_with_pandas.py` 
     uncomment only the following lines 
   1. with dataframe to sql
       ```
        ccn = CleanCompanyNames()
        ccn.run_program_df()
        ``` 
   2. with SQL update queries
       ```
       return df 
       ```
      
       ```
        ccn = CleanCompanyNames()
        ccn.run_program_sql()
        ```
      
   1.2. with the multiprocessing package - in `sqlite3_with_pandas.py`
       uncomment only the following lines
   1. with dataframe to sql
      ```
      ccn = CleanCompanyNames()
      ccn.with_multiprocessing_df()
      ```
   2. with SQL update queries
      ```
       return df 
       ```
      
       ```
       ccn = CleanCompanyNames()
       ccn.with_multiprocessing_sql()
       ```
   1.3. with the threading library - in `sqlite3_with_pandas.py`
           uncomment only the following lines 
   1. with dataframe to sql
       ```
       ccn = CleanCompanyNames()
       ccn.with_threading_df()
       ```
   2. with SQL update queries
       ```
       return df 
       ```
      
       ```
       ccn = CleanCompanyNames()
       ccn.with_threading_sql()
       ```
3. Print rows from the sqlite3 database - in `sqlite3_with_pandas.py`
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
3. In the methods that use SQL update queries
change the following line
    ```
    n = [i for i in range(0, 25000, 5000)]
    ```
    to define the start, end, and chunksize of a single query.
4. Use the replace() method for any additional occurrences
of substrings that need to be replaced.