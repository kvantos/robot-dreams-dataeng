import requests
import os

API_URL = "https://fake-api-vycpfa6oca-uc.a.run.app/"


class ApiException(Exception):
    pass


def is_blank(string):
    if string:
        return False
    return True


def get_sales(date: str, path: str):
    token = os.getenv('AUTH_TOKEN')
    if is_blank(token):
        raise ApiException("Auth Token not set. Exiting...")

    headers = {"Authorization": token}
    basedir = os.path.expanduser(
        '~/Projects/robot-dreams-dataeng/lesson7/data'
        )
    storing_path = os.path.join(basedir, path.lstrip('/'))

    if not os.path.exists(storing_path):
        try:
            os.makedirs(storing_path)
        except Exception as e:
            raise ApiException(f"Failed to create storage dir\n{e}")

    files = os.listdir(storing_path)
    if len(files) > 0:
        try:
            for file in files:
                os.remove(os.path.join(storing_path, file))
        except Exception as e:
            raise ApiException(f"Failed to clean storage\n{e}")

    page_n = 0
    while True:
        page_n += 1
        data = requests.get(
            f"{API_URL}sales?date={date}&page={page_n}",
            headers=headers
            )
        if data.status_code == 200:
            print(f'Read API [{date}] page {page_n}')
            file_name = f'sales_{date}_{page_n}.json'
            file_path = os.path.join(storing_path, file_name)
            with open(file_path, 'w') as fh:
                fh.write(data.text)
        elif data.status_code == 404:
            print("Read done.")
            break
        else:
            raise ApiException(f"Failed to connect API.\n{data.text}")
