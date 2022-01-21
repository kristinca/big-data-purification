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
        """ Connect to the semos_company_names sqlite3 database. """

        self.con = sqlite3.connect("semos_company_names.db")

    def purify_data(self):
        """
        Replace any substring in the column name.

        This function removes parentheses, hashes, question marks,
        brackets and text between brackets,
        as well as the following substrings:
            LIMITED, LLP, LIABILITY, LTD., LTD, PARTNERSHIP;
            dot, -THE, - THE (at the end of string).
        It also replaces multiple spaces from each row
        in the column name with a single space.
        """

        self.connect_to_db()

        df = pd.read_sql('''SELECT * FROM companies''', self.con)
        df['company_name_cleaned'] = df['name'] \
            .replace({'LTD': ''}, regex=True) \
            .replace({'LIMITED': ''}, regex=True) \
            .replace({'LLP': ''}, regex=True) \
            .replace({'LIABILITY': ''}, regex=True) \
            .replace({'PARTNERSHIP': ''}, regex=True) \
            .replace({'-THE$': ''}, regex=True) \
            .replace({'- THE$': ''}, regex=True) \
            .replace({'""$': ''}, regex=True) \
            .replace({'"': ' '}, regex=True) \
            .replace({'.$': ''}, regex=True) \
            .replace({'#': ''}, regex=True) \
            .replace({'/?/': ''}, regex=True) \
            .replace({'\[.*\]': ''}, regex=True) \
            .replace({'\(.*\)': ''}, regex=True) \
            .replace({'\(.*': ''}, regex=True) \
            .replace({'\s+': ' '}, regex=True)

        df['company_name_cleaned'] = df['company_name_cleaned'].str.title()
        df.to_sql('companies', self.con, chunksize=1000, if_exists='replace', index=False)

        self.con.close()

    def run_program(self):
        """
        Purification of big data in sqlite3 database
        without using the multiprocessing nor threading library.
        """

        start = time.perf_counter()

        self.purify_data()

        finish = time.perf_counter()
        print(f'\nPurification of big data finished in {round(finish - start, 2)} seconds.\n')

    def with_multiprocessing(self):
        """
        Purification of big data in sqlite3 database
        using the multiprocessing library.
        """

        start = time.perf_counter()

        p1 = Process(target=self.purify_data())

        p1.start()
        p1.join()

        finish = time.perf_counter()
        print(f'\nPurification of big data with the multiprocessing library '
              f'finished in {round(finish - start, 2)} seconds.\n')

    def with_threading(self):
        """
        Purification of big data in sqlite3 database
        using the threading library.
        """

        start = time.perf_counter()

        t1 = Thread(target=self.purify_data())

        t1.start()
        t1.join()

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

        self.con.close()


def main():
    """
    The main function to run this program.
    """

    # ccn = CleanCompanyNames()
    # ccn.run_program()
    # ccn.with_multiprocessing()
    # ccn.with_threading()
    # ccn.print_rows()
