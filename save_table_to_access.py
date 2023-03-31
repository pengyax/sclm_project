import pypyodbc
from shutil import copyfile
import numpy as np
import os


def save_table_to_access_db(dataframe, dbname, table_name):
    """[summary]

    Args:
        dataframe ([pd.DataFrame]): [要转存到access文件的df对象]
        dbname ([type]): [转存的access文件名]
    """    
    # 判断需要转换数据类型的列并完成转换
    todo_col = [i[0] for i in dataframe.dtypes.to_dict().items() if i[1]==np.dtype('float64')]
    todo_col = [i for i in todo_col if judge_is_integer(dataframe[i])]
    for i in todo_col:
        dataframe[i] = dataframe[i].fillna(0).astype('int64').replace(0, '').astype('str')
    # 储存到csv文件
    dataframe.to_csv("C:/t.csv", index=None, encoding='gbk')
    # 判断是否需要初始化文件
    signal = check_access_file_exsits(f"./", dbname)
    if signal:
        pass
    else:
    # 初始化本次任务的mdb文件
        copyfile("./base_db.mdb", f"./{dbname}.mdb")
    # 建立access数据库链接
    conn_str = (r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=./\%s.mdb;charset=utf-8;'%dbname)
    cnxn = pypyodbc.win_connect_mdb(conn_str)
    # 建立游标
    cur = cnxn.cursor()
    # 将 csv 文件内数据转存到access文件
    cur.execute(r"SELECT * INTO %s FROM [text;HDR=Yes;FMT=Delimited(|);charset=utf-8;Database=C:\].t.csv t"%table_name)
    # 提交并关闭链接
    cnxn.commit()
    cnxn.close()
    # 删除csv文件
    os.remove("C:/t.csv")
    return


def judge_is_integer(col):
    if len(col[col.notnull()])==0:
        return False
    a = (col%1).fillna(0)
    if len(a[a!=0])>0:
        return False
    else:
        return True
    
    
def check_access_file_exsits(path, dbname):
    l = [i for i in os.listdir(path) if i==dbname+'.mdb']
    if len(l)==1:
        return True
    else:
        return False
    