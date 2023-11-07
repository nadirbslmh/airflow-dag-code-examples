import os
import requests

from airflow.models.baseoperator import BaseOperator


def retrieve_posts(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()

        if response.status_code == 200:
            posts = response.json()
            return posts
        else:
            print(f"Unexpected status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while making the request: {e}")
    return None


def write_sql(posts, dirname, sql_filename):
    if posts:
        os.makedirs(dirname, exist_ok=True)
        sql_file = os.path.join(dirname, sql_filename)

        with open(sql_file, "w") as f:
            for post in posts:
                insert_query = f"INSERT INTO posts(id, userId, title, body) VALUES ({post['id']}, {post['userId']}, '{post['title']}', '{post['body']}');\n"
                f.writelines(insert_query)
    else:
        print("Failed to write SQL file")

    print("SQL insert queries have been written")


class CollectOperator(BaseOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        api_url = "https://jsonplaceholder.typicode.com/posts"

        posts = retrieve_posts(api_url)
        write_sql(posts, "dags/sql", "insert_data.sql")
