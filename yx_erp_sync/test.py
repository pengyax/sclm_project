from sql_engine import connect
import datetime
import pandas as pd


yx_con, server = connect('yx')
erp_con = connect('new_ERP')
bi_con = connect('bidata')