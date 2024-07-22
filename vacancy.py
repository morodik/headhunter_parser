import requests
from bs4 import BeautifulSoup
import fake_useragent
import time
import psycopg2
from psycopg2.extras import execute_values

def connect_db():
    try:
        conn = psycopg2.connect(
            dbname="vacansy_data",
            user="user",
            password="pass",
            host="host",
            port="port"
        )
        return conn
    except Exception as e:
        print(f"Ошибка при подключении к базе данных: {e}")
        return None
    
def get_first_vacancies():
    conn = connect_db()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT name, salary, description, city, link FROM vacancies LIMIT 5")
                vacancies = cursor.fetchall()
                return vacancies
        except Exception as e:
            print(f"Ошибка при выполнении запроса: {e}")
        finally:
            conn.close()
    return []



    
def get_links(text):
    try:
        ua = fake_useragent.UserAgent()
        data = requests.get(
            url=f"https://hh.ru/search/vacancy?text={text}&area=1&page=1",
            headers={"user-agent": ua.random}
        )
        if data.status_code != 200:
            return
        soup = BeautifulSoup(data.content, "lxml")
        try:
            page_count = int(soup.find("div", attrs={"class": "pager"}).find_all("span", recursive=False)[-1].find("a").find("span").text)
        except:
            return
        for page in range(page_count):
            data = requests.get(
                url=f"https://hh.ru/search/vacancy?text={text}&area=1&page={page}",
                headers={"user-agent": ua.random}
            )
            if data.status_code != 200:
                continue
            soup = BeautifulSoup(data.content, "lxml")
            for a in soup.find_all("a", attrs={"class": "bloko-link"}):
                link = a.attrs['href'].split('?')[0]
                if "vacancy/" in link:
                    yield link
    except Exception as e:
        print(f"{e}")
    time.sleep(1)


def get_vacancy(link):
    ua = fake_useragent.UserAgent()
    data = requests.get(
        url=link,
        headers={"user-agent": ua.random}
    )
    if data.status_code != 200:
        return
    soup = BeautifulSoup(data.content, "lxml")
    try:
        name = soup.find(attrs={"class": "bloko-header-section-1"}).text.strip()
    except:
        name = ""
    try:
        salary = soup.find(attrs={"class": "magritte-text___pbpft_3-0-9"}).text.replace("\u2009", "").replace("\xa0", " ").strip()
    except:
        salary = ""
    try:
        description = soup.find(attrs={"class": "g-user-content"}).text.strip()
    except:
        description = ""
    try:
        city = soup.find("span", {"data-qa": "vacancy-view-raw-address"}).text.strip()
    except:
        city = ""
    vacancy = {
        "Должность": name,
        "Зарплата": salary,
        "Описание": description,
        "Город": city,
        "Ссылка": link
    }
    return vacancy


def create_table(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vacancies (
                id SERIAL PRIMARY KEY,
                name TEXT,
                salary TEXT,
                description TEXT,
                city TEXT,
                link TEXT
            )
        """)
        conn.commit()

def insert_vacancy(conn, vacancy):
    try:
        with conn.cursor() as cursor:
            query = """
                INSERT INTO vacancies (name, salary, description, city, link)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(query, (vacancy["Должность"], vacancy["Зарплата"], vacancy["Описание"], vacancy["Город"], vacancy["Ссылка"]))
            conn.commit()
    except Exception as e:
        print(f"Ошибка при вставке данных: {e}")
        print(f"Данные: {vacancy}")
        conn.rollback()


if __name__ == "__main__":
    conn = psycopg2.connect(
        dbname="vacansy_data",
        user="user",
        password="pass",
        host="host",
        port="port"
    )
    
    create_table(conn)

    for a in get_links("название вакансии"):
        vacancy = get_vacancy(a)
        if vacancy:
            insert_vacancy(conn, vacancy)
            print(vacancy)
        time.sleep(1)

    conn.close()
