import sqlite3
import pandas as pd
from multiprocessing import Process
import tkinter as tk
import time


# 1. clean company names
class CleanCompanyNames:
    """ Purify big data from sqlite3 database """

    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def write_data_to_csv():
        """
        This function reads the data from sqlite3 database and writes it to a csv file.
        """

        con1 = sqlite3.connect("semos_company_names.db")
        df1 = pd.read_sql('''SELECT * FROM companies''', con1)

        start = time.perf_counter()
        df1.to_csv('the_data.csv', mode='w', index=False, chunksize=1000)
        finish = time.perf_counter()
        con1.close()

        print(f'\nWriting from sqlite3 database to a csv file finished in {round(finish - start, 2)} seconds.\n')

    @staticmethod
    def write_data_to_sqlite3_db():
        """
        This function reads the data from a csv file and writes it in a sqlite3 database.
        """

        con1 = sqlite3.connect("semos_company_names.db")

        start = time.perf_counter()
        the_data = pd.read_csv('the_data.csv')
        the_data.to_sql('companies', con1, index=False, if_exists='replace', chunksize=1000)

        finish = time.perf_counter()

        con1.close()

        print(f'\nWriting from csv file to a sqlite3 database finished in {round(finish - start, 2)} seconds.')

    @staticmethod
    def remove_parentheses_limited_ltd_etc():
        """
        Replace any substring in the column name.

        This function removes parentheses
        as well as the following substrings:
        LIMITED, LLP, LIABILITY, LTD., LTD, PARTNERSHIP, dot at the end of string.
        """

        start = time.perf_counter()
        df = pd.read_csv('the_data.csv')
        df['company_name_cleaned'] = df['name'] \
            .replace({'LTD': ''}, regex=True) \
            .replace({'LIMITED': ''}, regex=True) \
            .replace({'LLP': ''}, regex=True) \
            .replace({'LIABILITY': ''}, regex=True) \
            .replace({'PARTNERSHIP': ''}, regex=True) \
            .replace({'""$': ''}, regex=True) \
            .replace({'"': ' '}, regex=True) \
            .replace({'.$': ''}, regex=True) \
            .replace({'#': ''}, regex=True) \
            .replace({'/?/': ''}, regex=True)

        df['company_name_cleaned'] = df['company_name_cleaned'].str.title()
        df.to_csv('the_data.csv', index=False)
        finish = time.perf_counter()

        print(f'Removing parentheses limited etc from csv file finished in {round(finish - start, 2)} seconds.')

    @staticmethod
    def remove_brackets_and_text_in_between():
        """
        This function removes brackets and text between brackets
        from each row in the column name.
        """

        start = time.perf_counter()
        df = pd.read_csv('the_data.csv')
        df['company_name_cleaned'] = df['company_name_cleaned'] \
            .str.replace('\[.*\]', '',regex=True)\
            .replace('\(.*\)', '', regex=True)

        df.to_csv('the_data.csv', index=False)

        finish = time.perf_counter()

        print(f'Removing brackets and text between brackets from csv file finished in '
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
            .replace('\(.*', '', regex=True)\

        df.to_csv('the_data.csv', index=False)
        finish = time.perf_counter()

        print(f'Replacing multiple empty spaces with a single space finished in {round(finish - start, 2)} seconds.')

    def no_multiprocessing(self):
        """
        Purification of big data in sqlite3 database
        without using the multiprocessing library.
        """

        start = time.perf_counter()

        self.write_data_to_csv()
        self.remove_parentheses_limited_ltd_etc()
        self.remove_brackets_and_text_in_between()
        self.normalize_company_names()
        self.write_data_to_sqlite3_db()

        finish = time.perf_counter()
        print(f'\nPurification of big data without the multiprocessing library '
              f'finished in {round(finish - start, 2)} seconds.\n')

    def with_multiprocessing(self):
        """
        Purification of big data in sqlite3 database
        using the multiprocessing library
        (with multiprocessing.Process).
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


# 2. yield rows
class PrintRows:
    """
    Print rows from the table companies.
    """

    def __init__(self, *args, **kwargs):
        """
        Write the data from the sqlite3 database to a csv file.
        """

        ccn1 = CleanCompanyNames()
        ccn1.write_data_to_csv()

    @staticmethod
    def print_rows():
        """
        Print rows from the csv file.
        """

        df = pd.read_csv('the_data.csv')
        for row in df.itertuples(index=False, name=None):
            print(row)


# 3. gui
class MyApp(tk.Tk):
    """ The app """

    def __init__(self, *args, **kwargs):
        tk.Tk.__init__(self, *args, **kwargs)

        self.title('Company Names')
        self.geometry('600x250')
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self.resizable(False, False)

        container = tk.Frame(self)
        container.pack(side='top', fill='both', expand=True)
        container.grid_rowconfigure(0, weight=1)
        container.grid_columnconfigure(0, weight=1)
        container.config(bg='#086522')

        frame = tk.Frame(self)
        frame.pack()

        frame1 = FrameOne(parent=container, controller=self)
        frame1.pack()


class FrameOne(tk.Frame):
    """ The only frame """

    def __init__(self, parent, controller):
        tk.Frame.__init__(self, parent)

        self.controller = controller

        self.config(bg='#086522')

        company_names_label = tk.Label(self, text='Company Names', font='bold, 25', bg='#03C04A',
                                       borderwidth=5)
        company_names_label.grid(row=0, column=0, pady=20)

        clicked = tk.StringVar()

        drop = tk.OptionMenu(self, clicked, *self.sqlite_db())
        drop.grid(row=4, column=0)
        drop.config(width=40, bg='#03C04A', font=18)

    @staticmethod
    def sqlite_db():
        """
        A generator function.

        Connect to the sqlite3 database and yield rows.
        """

        con = sqlite3.connect("semos_company_names.db")
        df = pd.read_sql('''SELECT * FROM companies ORDER BY company_name_cleaned ASC ''', con, chunksize=1000)
        for chunk in df:
            for row in chunk.values:
                yield row[2]
        con.close()


class Main:
    """
    The main class.
    """

    def init(self, *args, **kwargs):
        pass

    @staticmethod
    def main():
        """
        The main function to run this program.
        """

    # 1. clean company names
        ccn = CleanCompanyNames()

    # without multiprocessing.Process
    #     ccn.no_multiprocessing()

    # with multiprocessing.Process
        ccn.with_multiprocessing()

    # 2. print rows from table companies
    #     yr = PrintRows()
    #     yr.print_rows()

    # 3. gui
    #     app = MyApp()
    #     app.mainloop()
