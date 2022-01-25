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
        # df.to_sql('companies', self.con, chunksize=1000, if_exists='replace', index=False)

        return df

    def update_sql(self, df1, b, a=0):

        for row in df1.values[a:b]:
            command = '''UPDATE companies SET company_name_cleaned = ? WHERE name = ?'''
            cursor = self.con.cursor()
            cursor.execute(command, [row[2], row[1]])
            self.con.commit()

    def run_program(self):
        """
        Purification of big data in sqlite3 database
        without using the multiprocessing nor threading library.
        """

        ddf = self.purify_data()

        start = time.perf_counter()

        # n = [i for i in range(0, 25000, 5000)]
        n = [i for i in range(0, 21000, 1000)]

        # n = [i for i in range(0, 20500, 500)]
        for ind in range(0, len(n)-1):
            if ind == 0:
                self.update_sql(ddf, n[1])
            else:
                self.update_sql(ddf, n[ind]+1, n[ind+1])
        self.con.close()

        finish = time.perf_counter()
        print(f'\nPurification of big data finished in {round(finish - start, 2)} seconds.\n')

    def with_multiprocessing(self):
        """
        Purification of big data in sqlite3 database
        using the multiprocessing library.
        """

        ddf = self.purify_data()

        start = time.perf_counter()

        n = [i for i in range(0, 21000, 1000)]

        p = dict()
        for ind in range(0, len(n)-1):
            if ind == 0:
                p[ind+1] = Process(target=self.update_sql(ddf, n[1]))
            else:
                p[ind+1] = Process(target=self.update_sql(ddf, n[ind]+1, n[ind+1]))

        for k in p.keys():
            p[k].start()
            print(f'{k}-th process started\n')

        for k in p.keys():
            p[k].join()
            print(f'{k}-th process ended\n')

        self.con.close()

        finish = time.perf_counter()
        print(f'\nPurification of big data with the multiprocessing library '
              f'finished in {round(finish - start, 2)} seconds.\n')

    def with_threading(self):
        """
        Purification of big data in sqlite3 database
        using the threading library.
        """

        ddf = self.purify_data()

        start = time.perf_counter()

        n = [i for i in range(0, 21000, 1000)]

        t = dict()
        for ind in range(0, len(n)-1):
            if ind == 0:
                t[ind+1] = Thread(target=self.update_sql(ddf, n[1]))
            else:
                t[ind+1] = Thread(target=self.update_sql(ddf, n[ind]+1, n[ind+1]))

        for k in t.keys():
            t[k].start()
            print(f'{k}-th thread started\n')

        for k in t.keys():
            t[k].join()
            print(f'{k}-th thread ended\n')

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
