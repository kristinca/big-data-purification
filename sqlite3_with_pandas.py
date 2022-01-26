import sqlite3
from threading import Thread
import time
import pandas as pd
from multiprocessing import Process


class CleanCompanyNames:
    """ Purify big data from sqlite3 database """

    def __init__(self, *args, **kwargs):
        self.con = None

    def connect_to_db(self):
        """ Connect to the semos_company_names sqlite3 database. """

        self.con = sqlite3.connect("semos_company_names.db")

    def purify_data(self):
        """
        Replace substrings in the column 'name'.

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
        # with dataframe to sql
        df.to_sql('companies', self.con, chunksize=1000, if_exists='replace', index=False)

        # with SQL update queries
        # return df

    def update_sql(self, df1):
        """
        Update sqlite3 db with SQL update query.
        """

        self.connect_to_db()
        for row in df1:
            command = '''UPDATE companies SET company_name_cleaned = ? WHERE name = ?'''
            cursor = self.con.cursor()
            cursor.execute(command, [row[2], row[1]])
            self.con.commit()
        self.con.close()

    def run_program_df(self):
        """
        Purification of big data in sqlite3 database
        without using the multiprocessing nor threading library.
        With dataframe to sql.
        """

        start = time.perf_counter()
        self.purify_data()
        finish = time.perf_counter()
        print(f"\nPurification of big data finished in {round(finish - start, 2)} seconds.\n")

    def run_program_sql(self):
        """
        Purification of big data in sqlite3 database
        without using the multiprocessing nor threading library.
        With SQL update queries.
        """

        start = time.perf_counter()
        ddf = self.purify_data()
        n = [i for i in range(0, 25000, 5000)]
        for ind in range(0, len(n)-1):
            self.update_sql(ddf.values[n[ind]:n[ind+1]])
        finish = time.perf_counter()
        print(f"\nPurification of big data finished in {round(finish - start, 2)} seconds.\n")

    def with_multiprocessing_df(self):
        """
        Purification of big data in sqlite3 database
        using the multiprocessing library.
        With dataframe to sql.
        """

        start = time.perf_counter()
        p = Process(target=self.purify_data)
        p.start()
        p.join()
        finish = time.perf_counter()
        print(f'\nPurification of big data with the multiprocessing library '
              f'finished in {round(finish - start, 2)} seconds.\n')

    def with_multiprocessing_sql(self):
        """
        Purification of big data in sqlite3 database
        using the multiprocessing library.
        With SQL update queries.
        """

        start = time.perf_counter()
        ddf = self.purify_data()
        n = [i for i in range(0, 25000, 5000)]
        p = dict()
        for ind in range(0, len(n)-1):
            print(f'{ind+1} process starts\n')
            p[ind+1] = Process(target=self.update_sql(ddf.values[n[ind]:n[ind+1]]))
            p[ind+1].start()
            p[ind+1].join()
            print(f'{ind+1}. process ended\n')
        finish = time.perf_counter()
        print(f'\nPurification of big data with the multiprocessing library '
              f'finished in {round(finish - start, 2)} seconds.\n')

    def with_threading_df(self):
        """
        Purification of big data in sqlite3 database
        using the threading library.
        With dataframe to sql.
        """
        start = time.perf_counter()
        t = Thread(target=self.purify_data)
        t.start()
        t.join()
        finish = time.perf_counter()
        print(f'\nPurification of big data with the threading library '
              f'finished in {round(finish - start, 2)} seconds.\n')

    def with_threading_sql(self):
        """
        Purification of big data in sqlite3 database
        using the threading library.
        With SQL update queries.
        """
        start = time.perf_counter()
        ddf = self.purify_data()

        n = [i for i in range(0, 25000, 5000)]

        t = dict()
        for ind in range(0, len(n)-1):
            print(f'{ind+1} thread starts\n')
            t[ind+1] = Thread(target=self.update_sql(ddf.values[n[ind]:n[ind+1]]))
            t[ind+1].start()
            t[ind+1].join()
            print(f'{ind+1} thread ended\n')
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
    # ccn.run_program_df()
    # ccn.run_program_sql()
    # ccn.with_multiprocessing_df()
    # ccn.with_multiprocessing_sql()
    # ccn.with_threading_df()
    # ccn.with_threading_sql()
    # ccn.print_rows()
