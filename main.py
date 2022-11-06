from pyhive import hive

host_name = "192.168.55.101"
port = 22
user = "cloudera"
password = "cloudera"
database = "demo"


def get_hive_connection(host_name, port, user, password, database):
    conn = hive.Connection(host=host_name, port=port, username=user, password=password,
                           database=database, auth='CUSTOM')
    cur = conn.cursor()

    return cur


def filter():
    cur = get_hive_connection(host_name, port, user, password, database)
    cur.execute('Select * from demo.customers where id<5')
    rs = cur.fetchall()
    print(rs)


def drop_table():
    cur = get_hive_connection(host_name, port, user, password, database)
    cur.execute('drop table demo.customers')


def join():
    cur = get_hive_connection(host_name, port, user, password, database)
    cur.execute('Select C.id,C.name,o.oid,o.order_date,o.amount from customers join orders o on c.id = o.customer_id')
    rs = cur.fetchall()
    print(rs)


filter()
join()
drop_table()
