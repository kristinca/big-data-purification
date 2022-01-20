import sqlite3
import pandas as pd
from multiprocessing import Process
from threading import Thread
import time


class CleanCompanyNames:
    """ Purify big data from sqlite3 database """

    def __init__(self, *args, **kwargs):
        self.con = None

    def connect_to_db(self):
        """
        Connect to the semos_company_names sqlite3 database.
        """

        self.con = sqlite3.connect("semos_company_names.db")

    @staticmethod
    def read_from_csv():
        """
        This function returns the data from the csv file 'the_data'.
        """

        return pd.read_csv('the_data.csv')

    def write_data_to_csv(self):
        """
        Read the data from the sqlite3 database,
        create csv file and write the data to this csv file.
        """

        self.connect_to_db()

        df1 = pd.read_sql('''SELECT * FROM companies''', self.con)

        start = time.perf_counter()
        df1.to_csv('the_data.csv', mode='w', index=False, chunksize=1000)
        finish = time.perf_counter()
        self.con.close()

        print(f'\nWriting from sqlite3 database to a csv file finished in {round(finish - start, 2)} seconds.\n')

    def write_data_to_sqlite3_db(self):
        """
        This function reads the data from the csv file 'the_data.csv' and writes it in a sqlite3 database.
        """

        self.connect_to_db()

        start = time.perf_counter()
        the_data = self.read_from_csv()
        the_data.to_sql('companies', self.con, index=False, if_exists='replace', chunksize=1000)

        finish = time.perf_counter()

        self.con.close()

        print(f'\nWriting from csv file to a sqlite3 database finished in {round(finish - start, 2)} seconds.')

    def remove_parentheses_limited_ltd_etc(self):
        """
        Replace any substring in the column name.

        This function removes parentheses, hashes, question marks,
        as well as the following substrings:
            LIMITED, LLP, LIABILITY, LTD., LTD, PARTNERSHIP;
            dot, -THE (at the end of string).
        """

        start = time.perf_counter()

        df = self.read_from_csv()
        df['company_name_cleaned'] = df['name'] \
            .replace({'LTD': ''}, regex=True) \
            .replace({'LIMITED': ''}, regex=True) \
            .replace({'LLP': ''}, regex=True) \
            .replace({'LIABILITY': ''}, regex=True) \
            .replace({'PARTNERSHIP': ''}, regex=True) \
            .replace({'-THE$': ''}, regex=True) \
            .replace({'""$': ''}, regex=True) \
            .replace({'"': ' '}, regex=True) \
            .replace({'.$': ''}, regex=True) \
            .replace({'#': ''}, regex=True) \
            .replace({'/?/': ''}, regex=True)

        df['company_name_cleaned'] = df['company_name_cleaned'].str.title()
        df.to_csv('the_data.csv', index=False)

        finish = time.perf_counter()

        print(f'Removing parentheses limited etc from csv file finished in {round(finish - start, 2)} seconds.')

    def remove_brackets_and_text_in_between(self):
        """
        This function removes brackets and text between brackets
        from each row in the column name.
        """

        start = time.perf_counter()
        df = self.read_from_csv()
        df['company_name_cleaned'] = df['company_name_cleaned'] \
            .replace({'\[.*\]': ''}, regex=True) \
            .replace({'\(.*\)': ''}, regex=True) \
            .replace({'\(.*': ''}, regex=True)

        df.to_csv('the_data.csv', index=False)

        finish = time.perf_counter()

        print(f'Removing brackets and text between brackets from csv file finished in '
              f'{round(finish - start, 2)} seconds.')

    def normalize_company_names(self):
        """
        This function replaces multiple spaces
        from each row in the column name
        with a single space.
        """

        start = time.perf_counter()
        df = self.read_from_csv()
        df['company_name_cleaned'] = df['company_name_cleaned'] \
            .replace({'\s+': ' '}, regex=True)

        df.to_csv('the_data.csv', index=False)
        finish = time.perf_counter()

        print(f'Replacing multiple empty spaces with a single space finished in {round(finish - start, 2)} seconds.')

    def run_program(self):
        """
        Purification of big data in sqlite3 database
        without using the multiprocessing nor threading library.
        """

        start = time.perf_counter()

        self.write_data_to_csv()
        self.remove_parentheses_limited_ltd_etc()
        self.remove_brackets_and_text_in_between()
        self.normalize_company_names()
        self.write_data_to_sqlite3_db()

        finish = time.perf_counter()

        print(f'\nPurification of big data finished in {round(finish - start, 2)} seconds.\n')

    def with_multiprocessing(self):
        """
        Purification of big data in sqlite3 database
        using the multiprocessing library.
        """

        start = time.perf_counter()

        p1 = Process(target=self.write_data_to_csv)
        p2 = Process(target=self.remove_parentheses_limited_ltd_etc)
        p3 = Process(target=self.remove_brackets_and_text_in_between)
        p4 = Process(target=self.normalize_company_names)
        p5 = Process(target=self.write_data_to_sqlite3_db)

        p1.start()
        p1.join()

        p2.start()
        p2.join()

        p3.start()
        p3.join()

        p4.start()
        p4.join()

        p5.start()
        p5.join()

        finish = time.perf_counter()
        print(f'\nPurification of big data with the multiprocessing library '
              f'finished in {round(finish - start, 2)} seconds.\n')

    def with_threading(self):
        """
        Purification of big data in sqlite3 database
        using the threading library.
        """

        start = time.perf_counter()

        t1 = Thread(target=self.write_data_to_csv)
        t2 = Thread(target=self.remove_parentheses_limited_ltd_etc)
        t3 = Thread(target=self.remove_brackets_and_text_in_between)
        t4 = Thread(target=self.normalize_company_names)
        t5 = Thread(target=self.write_data_to_sqlite3_db)

        t1.start()
        t1.join()

        t2.start()
        t2.join()

        t3.start()
        t3.join()

        t4.start()
        t4.join()

        t5.start()
        t5.join()

        finish = time.perf_counter()
        print(f'\nPurification of big data with the threading library '
              f'finished in {round(finish - start, 2)} seconds.\n')

    def print_rows(self):
        """
        Read sqlite3 to a dataframe.
        Print rows from the dataframe.
        """

        self.connect_to_db()

        df1 = pd.read_sql('''SELECT * FROM companies''', self.con, chunksize=1000)

        for chunk in df1:
            for row in chunk.values:
                print(str(row).replace('[', '').replace(']', ''))


def main():
    """
    The main function to run this program.
    """

    # ccn = CleanCompanyNames()
    # ccn.run_program()
    # ccn.with_multiprocessing()
    # ccn.with_threading()
    # ccn.print_rows()
