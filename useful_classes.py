import sqlite3
import pandas as pd
import re
from multiprocessing import Process
import tkinter as tk
import time


# 1. clean company names
class CleanCompanyNames:
    """ Purify big data from sqlite3 database """

    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def remove_parentheses_limited_ltd_etc():
        """
        Replace any substring in the column name.

        This function removes parentheses
        as well as the following substrings:
        LIMITED, LLP, LIABILITY, LTD., LTD, PARTNERSHIP.
        """

        con = sqlite3.connect("semos_company_names.db")
        cursor = con.cursor()

        df = pd.read_sql('''SELECT * FROM companies''', con, chunksize=1000)

        for chunk in df:
            for row in chunk.values:
                command = ''' UPDATE companies SET company_name_cleaned = ? WHERE name = ? '''
                cursor.execute(command, [str(row[1]).title().replace('Limited', '').replace('Llp', '').
                               replace('Liability', '').replace('Ltd.', '').replace('Ltd', '').
                               replace('Partnership', '').replace('"', ''), str(row[1])])
                con.commit()
        con.close()

    @staticmethod
    def remove_brackets_and_text_in_between():
        """
        This function removes brackets and text between brackets
        from each row in the column name.
        """

        con1 = sqlite3.connect("semos_company_names.db")
        cursor1 = con1.cursor()

        df = pd.read_sql('''SELECT * FROM companies''', con1, chunksize=1000)

        for chunk in df:
            for row in chunk.values:
                command1 = ''' UPDATE companies SET company_name_cleaned = ? WHERE company_name_cleaned = ? '''
                cursor1.execute(command1, [re.sub("[\(\[].*?[\)\]]", "", str(row[2])), str(row[2])])
                con1.commit()
        con1.close()

    @staticmethod
    def normalize_company_names():
        """
        This function replaces multiple spaces
        from each row in the column name
        with a single space.
        """

        con2 = sqlite3.connect("semos_company_names.db")
        cursor2 = con2.cursor()

        df = pd.read_sql('''SELECT * FROM companies''', con2, chunksize=1000)

        for chunk in df:
            for row in chunk.values:
                command2 = '''UPDATE companies SET company_name_cleaned = ? WHERE company_name_cleaned = ?'''
                cursor2.execute(command2, [re.sub(' +', ' ', str(row[2])), str(row[2])])
                con2.commit()
        con2.close()

    def no_multiprocessing(self):
        """
        Purification of big data in sqlite3 database
        without using the multiprocessing library.
        """

        start = time.perf_counter()

        start1 = time.perf_counter()
        self.remove_parentheses_limited_ltd_etc()
        finish1 = time.perf_counter()
        print(f'Finished in {round(finish1 - start1, 2)} seconds')

        start2 = time.perf_counter()
        self.remove_brackets_and_text_in_between()
        finish2 = time.perf_counter()
        print(f'Finished in {round(finish2 - start2, 2)} seconds')

        start3 = time.perf_counter()
        self.normalize_company_names()
        finish3 = time.perf_counter()
        print(f'Finished in {round(finish3 - start3, 2)} seconds')

        finish = time.perf_counter()
        print(f'\nPurification of big data finished in {round(finish - start, 2)} seconds')

    def with_multiprocessing(self):
        """
        Purification of big data in sqlite3 database
        using the multiprocessing library
        (with multiprocessing.Process).
        """

        start = time.perf_counter()

        start1 = time.perf_counter()
        p1 = Process(target=self.remove_parentheses_limited_ltd_etc)
        p1.start()
        print('first process started')
        p1.join()
        finish1 = time.perf_counter()
        print(f'Finished in {round(finish1 - start1, 2)} seconds')

        start2 = time.perf_counter()
        p2 = Process(target=self.remove_brackets_and_text_in_between)
        p2.start()
        print('second process started')
        p2.join()
        finish2 = time.perf_counter()
        print(f'Finished in {round(finish2 - start2, 2)} seconds')

        start3 = time.perf_counter()
        p3 = Process(target=self.normalize_company_names)
        p3.start()
        print('third process started')
        p3.join()
        finish3 = time.perf_counter()
        print(f'Finished in {round(finish3 - start3, 2)} seconds')

        finish = time.perf_counter()
        print(f'\nPurification of big data finished in {round(finish - start, 2)} seconds')


# 2. yield rows
class YieldRows:
    """
    Yield rows from the table companies.
    """

    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def yield_row():
        """
        A generator function.

        Connect to the sqlite3 database and yield rows.
        """

        con = sqlite3.connect("semos_company_names.db")

        df = pd.read_sql('''SELECT * FROM companies''', con, chunksize=1000)

        for chunk in df:
            for row1 in chunk.values:
                yield row1

        con.close()


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
    #     ccn = CleanCompanyNames()

    # without multiprocessing.Process
    #     ccn.no_multiprocessing()

    # with multiprocessing.Process
    #     ccn.with_multiprocessing()

    # 2. print rows from table companies
    #     yr = YieldRows()
    #     for row in yr.yield_row():
    #         print(row)

    # 3. gui
        app = MyApp()
        app.mainloop()
