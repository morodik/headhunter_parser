import requests
from bs4 import BeautifulSoup
import fake_useragent
import time
import psycopg2
from psycopg2.extras import execute_values


def connect_db():
    try:
        conn = psycopg2.connect(
            dbname="resume_data",
            user="user",
            password="pass",
            host="host",
            port="pass"
        )
        return conn
    except Exception as e:
        print(f"Ошибка при подключении к базе данных: {e}")
        return None


def create_table(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS resumes (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    salary TEXT,
                    tags TEXT[],
                    city TEXT,
                    link TEXT
                )
            """)
            conn.commit()
    except Exception as e:
        print(f"Ошибка при создании таблицы: {e}")

def get_first_resumes():
    conn = connect_db()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT name, salary, tags, city, link FROM resumes LIMIT 5")
                resumes = cursor.fetchall()
                return resumes
        except Exception as e:
            print(f"Ошибка при выполнении запроса: {e}")
        finally:
            conn.close()
    return []



def get_links(text):
    try:
        uagent = fake_useragent.UserAgent()
        data = requests.get(
            url=f"https://hh.ru/search/resume?text={text}&area=1&isDefaultArea=true&exp_period=all_time&logic=normal&pos=full_text&page=1",
            headers={"user-agent":uagent.random}
            )
        if data.status_code != 200:
            return 
        soup = BeautifulSoup(data.content, "lxml")
        try:
            page_count = int(soup.find("div", attrs={"class":"pager"}).find_all("span", recursive=False)[-1].find("a").find("span").text)
        except:
            return 
        for page in range(page_count):
            data = requests.get(
            url=f"https://hh.ru/search/resume?text={text}&area=1&isDefaultArea=true&exp_period=all_time&logic=normal&pos=full_text&page={page}",
            headers={"user-agent":uagent.random}
            )
            if data.status_code != 200:
                continue
            soup = BeautifulSoup(data.content, "lxml")
            for a in soup.find_all("a", attrs={"class":"bloko-link"}):
                yield f"https://hh.ru{a.attrs['href'].split('?')[0]}"
    except Exception as e:
        print(f"{e}")
    time.sleep(1)

def get_resume(link):
    uagent = fake_useragent.UserAgent()
    data = requests.get(
        url=link,
        headers={"user-agent":uagent.random}
    )
    if data.status_code != 200:
        return 
    soup = BeautifulSoup(data.content, "lxml")
    try:
        name = soup.find(attrs={"class":"resume-block__title-text"}).text.strip()
    except:
        name = ""
    try:
        salary = soup.find(attrs={"class":"resume-block__salary"}).text.replace("\u2009", "").replace("\xa0", " ").strip()
    except:
        salary = ""
    try:
        tags = [tag.text.strip() for tag in soup.find(attrs={"class": "bloko-tag-list"}).find_all(attrs={"class": "bloko-tag__section_text"})]
    except:
        tags = []
    try:
        city = soup.find("span", {"data-qa": "resume-personal-address"}).text.strip()
    except:
        city = ""
    resume = {
        "name": name,
        "salary": salary,
        "tags": tags,
        "city": city,
        "link": link
    }
    return resume


def insert_resume(conn, resume):
    if not (resume["name"] or resume["salary"] or resume["tags"] and resume["city"]):
        print("Пропуск вставки пустого резюме")
        return
    
    try:
        with conn.cursor() as cursor:
            query = """
                INSERT INTO resumes (name, salary, tags, city, link)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(query, (resume["name"], resume["salary"], resume["tags"], resume["city"], resume["link"]))
            conn.commit()
    except Exception as e:
        print(f"Ошибка при вставке данных: {e}")
        print(f"Данные: {resume}")
        conn.rollback()

if __name__ == "__main__":
    conn = connect_db()
    if conn:
        create_table(conn)
        
        for a in get_links("название резюме"):
            print(a)
            resume = get_resume(a)
            if resume:
                print(resume)
                insert_resume(conn, resume)
            time.sleep(1)
        conn.close()