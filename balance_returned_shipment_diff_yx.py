from sql_engine import connect
import pandas as pd
import datetime


def transdate(date,diff):
    end_date = date
    diff_date = datetime.timedelta(diff)
    start_date = end_date - diff_date
    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    return start_date,end_date


def load_return_data(std,edt):
    
    
    
    pass




def main():
    date = datetime.datetime.today()
    a,b = transdate(date,5)
    
    print(f'{a}是',type(a))
    print(f'{b}是',type(b))


if __name__ == '__main__':
    main()