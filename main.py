import sqlite3
import pandas as pd
import tkinter as tk
import re


# def main():
#     con = sqlite3.connect("semos_company_names.db")
#     cursor = con.cursor()
#
#     df = pd.read_sql('''SELECT * FROM companies''', con, chunksize=1000)
#
#     for chunk in df:
#         for row in chunk.values:
            # 1. clean company names:
            #
            # 1.1. remove "", limited, ltd, etc.
            # command = ''' UPDATE companies SET company_name_cleaned = ? WHERE name = ? '''
            # cursor.execute(command, [str(row[1]).title().replace('Limited', '').
            #                replace('Ltd.', '').replace('Ltd', '').replace('"', ''), str(row[1])])
            # con.commit()
            #
            # 1.2. remove brackets & text in between
            # command = ''' UPDATE companies SET company_name_cleaned = ? WHERE company_name_cleaned = ? '''
            # cursor.execute(command, [re.sub("[\(\[].*?[\)\]]", "", str(row[2])), str(row[2])])
            # con.commit()
            #
            # 1.3. normalize company name
            # command2 = '''UPDATE companies SET company_name_cleaned = ? WHERE company_name_cleaned = ?'''
            # cursor.execute(command2, [re.sub(' +', ' ', str(row[2])), str(row[2])])
            # con.commit()
            #
            # 2. yield rows
            # yield row

    # con.close()


# 3. gui
class MyApp(tk.Tk):
    """ The app """

    def __init__(self, *args, **kwargs):
        tk.Tk.__init__(self, *args, **kwargs)

        self.title('Company Names')
        self.iconbitmap('')
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

    def sqlite_db(self):
        con = sqlite3.connect("semos_company_names.db")
        cursor = con.cursor()
        df = pd.read_sql('''SELECT * FROM companies ORDER BY company_name_cleaned ASC ''', con, chunksize=1000)
        for chunk in df:
            for row in chunk.values:
                yield row[2]
        con.close()


if __name__ == '__main__':
    # 1. clean company names
    # main()

    # 2. print rows from table companies
    # for row in main():
    #     print(row)

    # 3. gui
    app = MyApp()
    app.mainloop()
