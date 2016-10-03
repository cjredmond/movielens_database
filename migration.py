import csv
import psycopg2

connection = psycopg2.connect("dbname=movielens user=movielens")
cursor = connection.cursor()

cursor.execute("DROP TABLE IF EXISTS user_data;")

create_user_table_command = """
CREATE TABLE user_data (
    id INT PRIMARY KEY NOT NULL,
    age INT,
    gender VARCHAR(1),
    occupation VARCHAR(30),
    zip VARCHAR(8)
);

"""
cursor.execute(create_user_table_command)
with open('user.csv') as csvfile:
    read = csv.reader(csvfile, delimiter='|')
    for row in read:
        cursor.execute("INSERT INTO user_data VALUES(%s,%s,%s,%s,%s)",(row[:]))
connection.commit()



cursor.execute("DROP TABLE IF EXISTS item_data;")
create_item_table_command = """
CREATE TABLE item_data (
    movie_id SERIAL PRIMARY KEY NOT NULL,
    title VARCHAR(100),
    release_date VARCHAR(100),
    video_release VARCHAR(100),
    URL VARCHAR(150),
    unknown VARCHAR(150),
    action_genre INT,
    adventure INT,
    animation INT,
    childrens INT,
    comedy INT,
    crime INT,
    documentary INT,
    drama INT,
    fantasy INT,
    film_noir INT,
    horror INT,
    musical INT,
    mystery INT,
    romance INT,
    sci_fi INT,
    thriller INT,
    war INT,
    western INT
);
"""
cursor.execute(create_item_table_command)
with open('item.csv') as csvfile:
    read_item = csv.reader(csvfile, delimiter='|')
    for row in read_item:
        cursor.execute("INSERT INTO item_data VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",row)
connection.commit()

cursor.execute("DROP TABLE IF EXISTS data;")
create_data_table_command = """
CREATE TABLE data (
    user_id INT references user_data(id),
    item_id INT references item_data(movie_id),
    rating INT,
    time_stamp INT
);
"""
cursor.execute(create_data_table_command)
with open('data.csv') as csvfile:
    read_data = csv.reader(csvfile, delimiter='\t')
    for row in read_data:
        cursor.execute("INSERT INTO data VALUES(%s,%s,%s,%s)",(row[:]))
connection.commit()




cursor.close()
connection.close()
