#!/usr/bin/env python3

"""
1. Написати джобу, яка витягує всі данні продаж з API та зберігає їх в raw директорію:
* В якості шаблону для джоби використати код в папці: [link]
* Приклад, як діставати дані з API: [link]

* В файлі main.py має бути реалізований WEB сервер за допомогою фреймворка Flask,
    який працює локально і приймає команди у вигляді POST запитів на порт 8081
* Сервер має приймати JSON обʼєкт наступного вигляду: [link]
* Шлях зберігання має мати вигляд: /path/to/my_dir/raw/sales/2022-08-09. 
    Цей шлях має подаватися у вигляді параметру raw_dir для джоби.
* Джоба обовʼязково має бути ідемпотентною. Для цього джоба сама має очищати 
    вміст директорії raw_dir перед записом нових файлів
* Кожну “сторінку” з API можна зберігати в окремому файлі, або всі сторінки в одному великому JSON.
    Файл має мати імʼя sales_2022-08-09.json або sales_2022-08-09_1.json, sales_2022-08-09_2.json
    і т.д. якщо файлів декілька
"""


from flask import Flask
import requests
import os

app = Flask(__name__)

api_url = "https://fake-api-vycpfa6oca-uc.a.run.app/"
path = "sales/2022-08-09"


def is_blank(string):
    if string:
        return False
    return True


def fetch_data(path):
    token = os.getenv('AUTH_TOKEN')
    if is_blank(token):
        print("Token not set. Exiting...")
        return
    
    headers = {"Authorization": token}

    if not os.path.exists(path):
        os.makedirs(os.path.join('raw', path))

    page_n = 0
    while True:
        page_n += 1
        data = requests.get(f"{api_url}sales?date=2022-08-09&page={page_n}", headers=headers)
        if data.status_code == 200:
            file_path = os.path.join(path, f'sales_2022-08-09_{page_n}.json')
            with open(file_path, 'w') as fh:
                fh.write(data.text)
        elif data.status_code == 404:
            print("Read done.")
            break
        else:
            print(data.text)
            print("Failed to connect API. Please check connection parameters")
            break

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

if __name__ == "__main__":
    app.run(port=8081, debug=True)
