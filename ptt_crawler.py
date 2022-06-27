from beartype import beartype
from bs4 import BeautifulSoup
import functools
import requests
import sqlite3
import time


def rollback(func):
    def wrapper_func(cls, *args, **kwargs):

        try:
            result = func(cls, *args, **kwargs)
            if bool(result) is True:
                return result

        except Exception as e:
            print(e)
            cls.conn.rollback()

    return functools.update_wrapper(wrapper_func, func)


class sqlTool:
    cur = None
    conn = None

    @classmethod
    @beartype
    def open_connection(cls):
        db_name = './ptt.db'
        timeout = 30000
        conn = sqlite3.connect(db_name,
                               isolation_level=None,
                               check_same_thread=False,
                               timeout=timeout)
        cur = conn.cursor()

        cls.cur = cur
        cls.conn = conn

    @classmethod
    @beartype
    def close_database_connection(cls):
        cls.cur.close()
        cls.conn.close()

    @classmethod
    @beartype
    def insert_data(cls, table_name: str, data_list: list):
        sql = '''
              INSERT INTO {}
              (url, state)
              VALUES ("{}",{})
              '''
        cls.open_connection()

        for data in data_list:
            exec_sql = sql.format(table_name, data['url'], data['state'])
            cls.cur.execute(exec_sql)
            cls.conn.commit()  # 提交資料

        cls.close_database_connection()


for i in range(1, 17549):
    table_name = 'C_Chat'
    try:
        url_ls = []

        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36"
        }

        url = f'https://www.ptt.cc/bbs/{table_name}/index{str(i)}.html'
        sess = requests.session()
        res = sess.get(url=url, headers=headers)

        soup = BeautifulSoup(res.content, 'lxml')
        rent_ls = soup.find_all(attrs={"class": "r-ent"})
        for rent in rent_ls:
            href = rent.find('a')
            if href is not None:
                url_ls += [{'url': href.get('href'), 'state': 0}]

        sqlTool.insert_data(table_name, url_ls)
        time.sleep(3)
    except Exception as e:
        print(f'index: {str(i)}')
        print(e)
