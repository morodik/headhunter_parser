import telebot
from telebot import types
import psycopg2
import logging
from vacancy import get_first_vacancies, get_vacancy, insert_vacancy, get_links as get_vacancy_links
from main import get_first_resumes, get_links as get_resume_links, get_resume, insert_resume

API_TOKEN = ''  

bot = telebot.TeleBot(API_TOKEN)


try:
    bot.get_me()
except Exception as e:
    logging.error(f"Ошибка проверки токена: {e}")
    exit(1)


user_state = {}

def vacancy_connect_db():
    try:
        conn = psycopg2.connect(
            dbname="vacansy_data",
            user="user",
            password="",
            host="host",
            port="port"
        )
        return conn
    except Exception as e:
        logging.error(f"Ошибка при подключении к базе данных: {e}")
        return None

def resume_connect_db():
    try:
        conn = psycopg2.connect(
            dbname="resume_data",
            user="user",
            password="pass",
            host="host",
            port="port"
        )
        return conn
    except Exception as e:
        logging.error(f"Ошибка при подключении к базе данных: {e}")
        return None

@bot.message_handler(commands=['start'])
def start(message):
    send_start_message(message.chat.id)

def send_start_message(chat_id):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("Вакансии", callback_data='vacancy'))
    markup.add(types.InlineKeyboardButton("Резюме", callback_data='resume'))
    bot.send_message(chat_id, "Что будем искать?", reply_markup=markup)

@bot.callback_query_handler(func=lambda callback: True)
def callback_message(callback):
    chat_id = callback.message.chat.id
    if callback.data == 'vacancy':
        user_state[chat_id] = {'type': 'vacancy', 'keyword': None, 'page': 1}
        bot.send_message(chat_id, "Введите название вакансии:")
    elif callback.data == 'resume':
        user_state[chat_id] = {'type': 'resume', 'keyword': None, 'page': 1}
        bot.send_message(chat_id, "Введите название должности для поиска резюме:")
    elif callback.data == 'more_vacancy':
        state = user_state.get(chat_id)
        if state and state['type'] == 'vacancy':
            state['page'] += 1
            search_vacancies(chat_id, state['keyword'], state['page'])
    elif callback.data == 'more_resume':
        state = user_state.get(chat_id)
        if state and state['type'] == 'resume':
            state['page'] += 1
            search_resumes(chat_id, state['keyword'], state['page'])
    elif callback.data == 'other_vacancies':
        user_state[chat_id] = {'type': 'vacancy', 'keyword': None, 'page': 1}
        send_start_message(chat_id)
    elif callback.data == 'other_resumes':
        user_state[chat_id] = {'type': 'resume', 'keyword': None, 'page': 1}
        send_start_message(chat_id)

@bot.message_handler(content_types=['text'])
def text_message(message):
    chat_id = message.chat.id
    text = message.text.strip()
    logging.info(f"Получен текст от пользователя: {text}")

    state = user_state.get(chat_id)

    if state:
        if state['type'] == 'vacancy':
            state['keyword'] = text
            search_vacancies(chat_id, text, state['page'])
        elif state['type'] == 'resume':
            state['keyword'] = text
            search_resumes(chat_id, text, state['page'])

def search_vacancies(chat_id, keyword, page):
    conn = vacancy_connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            offset = (page - 1) * 5
            query = "SELECT name, salary, description, city, link FROM vacancies WHERE name ILIKE %s LIMIT 5 OFFSET %s"
            cursor.execute(query, ('%' + keyword + '%', offset))
            vacancies = cursor.fetchall()
            if not vacancies and page == 1:
                bot.send_message(chat_id, "По вашему запросу в вакансиях ничего не найдено. Выполняется парсинг данных, пожалуйста, подождите...")
                vacancies = parse_and_store_vacancies(keyword)
            if vacancies:
                for vacancy in vacancies:
                    name, salary, description, city, link = vacancy
                    bot.send_message(chat_id, f"Должность: {name}\nЗарплата: {salary}\nОписание: {description}\nГород: {city}\nСсылка: {link}")
                if len(vacancies) == 5:
                    markup = types.InlineKeyboardMarkup()
                    markup.add(types.InlineKeyboardButton("Другие вакансии", callback_data='more_vacancy'))
                    markup.add(types.InlineKeyboardButton("В начало", callback_data='other_vacancies'))
                    bot.send_message(chat_id, "Что дальше?", reply_markup=markup)
            else:
                bot.send_message(chat_id, "К сожалению, ничего не найдено.")
            conn.close()
        except Exception as e:
            print(f"Ошибка при выполнении запроса: {e}")
            conn.close()
    else:
        bot.send_message(chat_id, "Ошибка подключения к базе данных.")

def search_resumes(chat_id, keyword, page):
    conn = resume_connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            offset = (page - 1) * 5
            query = "SELECT name, salary, tags, city, link FROM resumes WHERE name ILIKE %s LIMIT 5 OFFSET %s"
            cursor.execute(query, ('%' + keyword + '%', offset))
            resumes = cursor.fetchall()
            if not resumes and page == 1:
                bot.send_message(chat_id, "По вашему запросу в резюме ничего не найдено. Выполняется парсинг данных, пожалуйста, подождите...")
                resumes = parse_and_store_resumes(keyword)
            if resumes:
                for resume in resumes:
                    name, salary, tags, city, link = resume
                    bot.send_message(chat_id, f"Должность: {name}\nЗарплата: {salary}\nНавыки: {tags}\nГород: {city}\nСсылка: {link}")
                if len(resumes) == 5:
                    markup = types.InlineKeyboardMarkup()
                    markup.add(types.InlineKeyboardButton("Другие резюме", callback_data='more_resume'))
                    markup.add(types.InlineKeyboardButton("В начало", callback_data='other_resumes'))
                    bot.send_message(chat_id, "Что дальше?", reply_markup=markup)
            else:
                bot.send_message(chat_id, "К сожалению, ничего не найдено.")
            conn.close()
        except Exception as e:
            print(f"Ошибка при выполнении запроса: {e}")
            conn.close()
    else:
        bot.send_message(chat_id, "Ошибка подключения к базе данных.")

def parse_and_store_vacancies(keyword):
    conn = vacancy_connect_db()
    if conn:
        try:
            vacancies = []
            for link in get_vacancy_links(keyword):
                vacancy = get_vacancy(link)
                if vacancy:
                    insert_vacancy(conn, vacancy)
                    vacancies.append((vacancy["Должность"], vacancy["Зарплата"], vacancy["Описание"], vacancy["Город"], vacancy["Ссылка"]))
                    if len(vacancies) >= 5:
                        break
            conn.close()
            return vacancies
        except Exception as e:
            print(f"Ошибка при парсинге и сохранении вакансий: {e}")
            conn.close()
            return []
    else:
        return []

def parse_and_store_resumes(keyword):
    conn = resume_connect_db()
    if conn:
        try:
            resumes = []
            for link in get_resume_links(keyword):
                resume = get_resume(link)
                if resume:
                    insert_resume(conn, resume)
                    resumes.append((resume["name"], resume["salary"], resume["tags"], resume["city"], resume["link"]))
                    if len(resumes) >= 5:
                        break
            conn.close()
            return resumes
        except Exception as e:
            print(f"Ошибка при парсинге и сохранении резюме: {e}")
            conn.close()
            return []
    else:
        return []

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    bot.polling(none_stop=True)
