import sqlite3
import pandas as pd
import re
from multiprocessing import Process
import time


# 1. clean company names
class CleanCompanyNames:
    """ Purify big data from sqlite3 database """

    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def write_data_to_csv():
        con1 = sqlite3.connect("sn.db")
        df1 = pd.read_sql('''SELECT * FROM companies''', con1)

        start = time.perf_counter()
        df1.to_csv('the_data.csv', mode='w', index=False, chunksize=1000)
        finish = time.perf_counter()

        print(f'Writing from sqlite3 database to a csv file finished in {round(finish - start, 2)} seconds')

    @staticmethod
    def remove_parentheses_limited_ltd_etc():
        """
        Replace any substring in the column name.

        This function removes parentheses
        as well as the following substrings:
        LIMITED, LLP, LIABILITY, LTD., LTD, PARTNERSHIP.
        """

        start = time.perf_counter()
        df = pd.read_csv('the_data.csv')
        df['company_name_cleaned'] = df['name'] \
            .replace({'LTD': ''}, regex=True) \
            .replace({'LIMITED': ''}, regex=True) \
            .replace({'LLP': ''}, regex=True) \
            .replace({'LIABILITY': ''}, regex=True) \
            .replace({'PARTNERSHIP': ''}, regex=True) \
            .replace({'"': ''}, regex=True) \
            .replace({'.$': ""}, regex=True)

        df['company_name_cleaned'] = df['company_name_cleaned'].str.title()
        df.to_csv("the_data.csv", index=False)
        finish = time.perf_counter()

        print(f'Removing parentheses limited etc from csv file finished in {round(finish - start), 2} seconds.')

    @staticmethod
    def remove_brackets_and_text_in_between():
        """
        This function removes brackets and text between brackets
        from each row in the column name.
        """

        start = time.perf_counter()
        df = pd.read_csv('the_data.csv')
        df['company_name_cleaned'] = df['company_name_cleaned'] \
            .str.replace("\[.*\]", "",regex=True)\
            .replace("\(.*\)", "", regex=True)

        df.to_csv("the_data.csv", index=False)

        finish = time.perf_counter()

        print(f'Removing brackets and text between brackets from csv file finished in'
              f'{round(finish - start, 2)} seconds.')

    @staticmethod
    def normalize_company_names():
        """
        This function replaces multiple spaces
        from each row in the column name
        with a single space.
        """
        start = time.perf_counter()
        df = pd.read_csv('the_data.csv')
        df['company_name_cleaned'] = df['company_name_cleaned'] \
            .replace('\s+', ' ', regex=True)\
            .replace("\(.*", '', regex=True)

        df.to_csv("the_data.csv", index=False)
        print(df)
        finish = time.perf_counter()

        print(f'Replacing multiple empty spaces with a single space finished in {round(finish - start, 2)} seconds.')

    #
    # def no_multiprocessing():
    #     """
    #     Purification of big data in sqlite3 database
    #     without using the multiprocessing library.
    #     """
    #
    #     start = time.perf_counter()
    #
    #     start1 = time.perf_counter()
    #     .remove_parentheses_limited_ltd_etc()
    #     finish1 = time.perf_counter()
    #     print(f'Finished in {round(finish1 - start1, 2)} seconds')
    #
    #     start2 = time.perf_counter()
    #     .remove_brackets_and_text_in_between()
    #     finish2 = time.perf_counter()
    #     print(f'Finished in {round(finish2 - start2, 2)} seconds')
    #
    #     start3 = time.perf_counter()
    #     .normalize_company_names()
    #     finish3 = time.perf_counter()
    #     print(f'Finished in {round(finish3 - start3, 2)} seconds')
    #
    #     finish = time.perf_counter()
    #     print(f'\nPurification of big data finished in {round(finish - start, 2)} seconds')
    #
    # def with_multiprocessing():
    #     """
    #     Purification of big data in sqlite3 database
    #     using the multiprocessing library
    #     (with multiprocessing.Process).
    #     """
    #
    #     with open('the_data.csv', 'w') as f:
    #
    #     #
    #     # con2 = sqlite3.connect("sn.db")
    #     # cursor2 = con2.cursor()
    #
    #     df = pd.read_sql('''SELECT * FROM companies''', con2, chunksize=1000)
    #
    #     with open('the_data.csv', 'w') as sqlite3_to_csv:
    #         start = time.perf_counter()
    #         for chunk in df:
    #             for row in chunk.values:
    #                 filewriter = csv.writer(sqlite3_to_csv, delimiter=',')
    #                 filewriter.writerow(row)
    #         finish = time.perf_counter()
    #
    #     print(f'Writing from sqlite3 database to a csv file finished in {round(finish - start, 2)} seconds')
    #
    #     p1 = Process(target=.remove_parentheses_limited_ltd_etc)
    #     p2 = Process(target=.remove_brackets_and_text_in_between)
    #     p3 = Process(target=.normalize_company_names)
    #
    #     start_all = time.perf_counter()
    #     start1 = time.perf_counter()
    #     p1.start()
    #     print('first process started')
    #
    #     start2 = time.perf_counter()
    #     p2.start()
    #     print('second process started')
    #
    #     start3 = time.perf_counter()
    #     p3.start()
    #     print('third process started')
    #
    #     p1.join()
    #     finish1 = time.perf_counter()
    #     print(f'First process finished in {round(finish1 - start1, 2)} seconds')
    #
    #     p2.join()
    #     finish2 = time.perf_counter()
    #     print(f'Second process finished in {round(finish2 - start2, 2)} seconds')
    #
    #     p3.join()
    #     finish3 = time.perf_counter()
    #     print(f'Third process finished in {round(finish3 - start3, 2)} seconds')
    #
    #     finish_all = time.perf_counter()
    #     print(f'\nPurification of big data finished in {round(finish_all - start_all, 2)} seconds')
    #


if __name__ == '__main__':
    ccn = CleanCompanyNames()
    ccn.write_data_to_csv()
    ccn.remove_parentheses_limited_ltd_etc()
    ccn.remove_brackets_and_text_in_between()
    ccn.normalize_company_names()
