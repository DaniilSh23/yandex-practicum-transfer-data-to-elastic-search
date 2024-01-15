"""
АВТОРСКОЕ РЕШЕНИЕ.
"""

import json
import logging
import sqlite3
from contextlib import contextmanager
from typing import List
from urllib.parse import urljoin
import requests

logger = logging.getLogger()


def dict_factory(cursor: sqlite3.Cursor, row: tuple) -> dict:
    """
    Так как в SQLite нет встроенной фабрики для строк в виде dict,
    всё приходится делать самостоятельно
    """
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


@contextmanager
def conn_context(db_path: str):
    """
    В SQLite нет контекстного менеджера для работы с соединениями,
    поэтому добавляем его тут, чтобы грамотно закрывать соединения
    :param db_path: путь до базы данных
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = dict_factory
    yield conn
    conn.close()


class ESLoader:
    def __init__(self, url: str):
        self.url = url

    def _get_es_bulk_query(self, rows: List[dict], index_name: str) -> List[str]:
        """
        Подготавливает bulk-запрос в Elasticsearch
        """
        prepared_query = []
        for row in rows:
            prepared_query.extend([
                json.dumps({'index': {'_index': index_name, '_id': row['id']}}),
                json.dumps(row)
            ])
        return prepared_query

    def load_to_es(self, records: List[dict], index_name: str):
        """
        Отправка запроса в ES и разбор ошибок сохранения данных
        """
        prepared_query = self._get_es_bulk_query(records, index_name)
        str_query = '\n'.join(prepared_query) + '\n'

        url = urljoin(self.url, '_bulk')
        ca_cert_path = "http_ca.crt"
        elastic_password = "IMiAXp0Er1qb60sAEN3C"

        # Создаем объект сессии
        session = requests.Session()
        # Устанавливаем путь к CA-сертификату
        session.verify = ca_cert_path
        # Подготавливаем HTTP-аутентификацию
        auth = ("elastic", elastic_password)
        # Выполняем запрос с использованием объекта сессии
        response = session.post(url, auth=auth, data=str_query, headers={'Content-Type': 'application/x-ndjson'})

        json_response = json.loads(response.content.decode())
        for item in json_response['items']:
            error_message = item['index'].get('error')
            if error_message:
                logger.error(error_message)


class ETL:
    # Хуета, а не запрос, не актуален
    # SQL = '''
    #     /* Используем CTE для читаемости. Здесь нет прироста
    #     производительности, поэтому можно поменять на subquery */
    #     WITH x as (
    #     -- Используем group_concat, чтобы собрать id и имена
    #     всех актёров в один список после join'а с таблицей actors
    #     -- Отметим, что порядок id и имён совпадает
    #     -- Не стоит забывать про many-to-many связь между
    #     таблицами фильмов и актёров
    #     SELECT m.id, group_concat(a.id) as actors_ids, group_concat(a.name) as actors_names
    #     FROM movies m
    #     LEFT JOIN movie_actors ma on m.id = ma.movie_id
    #     LEFT JOIN actors a on ma.actor_id = a.id
    #     GROUP BY m.id
    #     )
    #     -- Получаем список всех фильмов со сценаристами и актёрами
    #     SELECT m.id, genre, director, title, plot, imdb_rating, x.actors_ids, x.actors_names,
    #     /* Этот CASE решает проблему в дизайне таблицы:
    #     если сценарист всего один, то он записан простой строкой
    #     в столбце writer и id. В противном случае данные
    #     хранятся в столбце writers и записаны в виде
    #     списка объектов JSON.
    #     Для исправления ситуации применяем хак:
    #     приводим одиночные записи сценаристов к списку
    #     из одного объекта JSON и кладём всё в поле writers */
    #     CASE
    #     WHEN m.writers = '' THEN '[{"id": "' || m.writer || '"}]'
    #     ELSE m.writers
    #     END AS writers
    #     FROM movies m
    #     LEFT JOIN x ON m.id = x.id
    # '''
    # Нормальный запрос
    MY_SQL_QUERY = """
    SELECT film_work.id AS id, film_work.rating AS rating, film_work.title AS title, 
    film_work.description AS description
    FROM film_work;
    """

    def __init__(self, conn: sqlite3.Connection, es_loader: ESLoader):
        self.es_loader = es_loader
        self.conn = conn

    # def _transform_row(self, row: dict, writers: dict) -> dict:
    #     """
    #     Основная логика преобразования данных из SQLite во внутреннее
    #     представление, которое дальше будет уходить в Elasticsearch
    #     Решаемы проблемы:
    #     1) genre в БД указан в виде строки из одного или нескольких
    #     жанров, разделённых запятыми -> преобразовываем в список жанров.
    #     2) writers из запроса в БД приходит в виде списка словарей id'шников
    #     -> обогащаем именами из полученных заранее сценаристов
    #     и добавляем к каждому id ещё и имя
    #     3) actors формируем из двух полей запроса (actors_ids и actors_names)
    #     в список словарей, наподобие списка сценаристов.
    #     4) для полей writers, imdb_rating, director и description меняем
    #     поля 'N/A' на None.
    #     :param row: строка из БД
    #     :param writers: текущие сценаристы
    #     :return: подходящая строка для сохранения в Elasticsearch
    #     """
    #     movie_writers = []
    #     writers_set = set()
    #     for writer in json.loads(row['writers']):
    #         writer_id = writer['id']
    #         if writers[writer_id]['name'] != 'N/A' and writer_id not in writers_set:
    #             movie_writers.append(writers[writer_id])
    #             writers_set.add(writer_id)
    #     actors = []
    #     if row['actors_ids'] is not None and row['actors_names'] is not None:
    #         actors = [
    #             {'id': _id, 'name': name}
    #             for _id, name in zip(row['actors_ids'].split(','), row['actors_names'].split(','))
    #             if name != 'N/A'
    #         ]
    #     actors_names = [x for x in row['actors_names'].split(',') if x != 'N/A']
    #     return {
    #         'id': row['id'],
    #         'genre': row['genre'].replace(' ', '').split(','),
    #         'writers': movie_writers,
    #         'actors': actors,
    #         'actors_names': actors_names,
    #         'writers_names': [x['name'] for x in movie_writers],
    #         'imdb_rating': float(row['imdb_rating']) if row['imdb_rating'] != 'N/A' else None,
    #         'title': row['title'],
    #         'director': [x.strip() for x in row['director'].split(',')] if row['director'] != 'N/A' else None,
    #         'description': row['plot'] if row['plot'] != 'N/A' else None
    #     }

    def load_persons_for_film(self, role: str) -> dict:
        """
        Получаем персон с определенной ролью для фильмов и упаковываем их в словарь в следующем виде
        {
            "film_work_id": [{"id": "person_id", "name": "person full_name"}, ...],
            ...
        }
        """
        sql_query = f"""
        SELECT person_film_work.film_work_id AS film_id, person_film_work.person_id AS id, 
        person.full_name AS name FROM person_film_work 
        INNER JOIN person ON person.id = person_film_work.person_id 
        WHERE person_film_work.role = '{role}';
        """
        result_data = dict()
        for person in self.conn.execute(sql_query):
            # Если ключа с фильмом еще нет, то создаем его
            if not result_data.get(person["film_id"]):
                result_data[person["film_id"]] = list()
            result_data[person["film_id"]].append({"id": person["id"], "name": person["name"]})
        return result_data

    def load_genres_names(self):
        """
        Получаем название жанров и кладем их в словарь, в котором ключом будет film_work_id.
        Так будет удобнее по фильму достать инфу о жанре.
        {
            "film_work_id": {"id": "genre_id", "name": "genre name"},
            ...
        }
        """
        sql_query = """
        SELECT genre.id AS id, genre.name AS name, genre_film_work.film_work_id AS film_id FROM genre
        INNER JOIN genre_film_work ON genre.id = genre_film_work.genre_id;
        """
        genres = {}
        for genre in self.conn.execute(sql_query):
            # Если ключа с фильмом еще нет, то создаем его
            if not genres.get(genre["film_id"]):
                genres[genre['film_id']] = list()
            genres[genre['film_id']].append({"id": genre["id"], "name": genre["name"]})
        return genres

    def load(self, index_name: str):
        """
        Основной метод для вашего ETL.
        Обязательно используйте метод load_to_es — это будет проверяться
        :param index_name: название индекса, в который будут грузиться данные
        """
        # Получаем писателей, актеров и директоров
        persons = dict()
        for i_role in ("writer", "actor", "director"):
            persons[i_role] = self.load_persons_for_film(role=i_role)

        # Достаем жанры
        genres = self.load_genres_names()

        records = []
        try:
            for film_work in self.conn.execute(self.MY_SQL_QUERY):
                transformed_row = self.data_packaging(film_work, persons, genres[film_work["id"]])
                records.append(transformed_row)
        except Exception as err:
            print(f'Пиздой накрылось все на фильме: {film_work} | Вот ошибка: {err}')
            return

        self.es_loader.load_to_es(records, index_name)

    def data_packaging(self, film_work, persons, film_genres):
        """
        Функция для упаковки данных. Берем данные из разных таблиц и упаковываем их в формат для создания одной записи
        в индексе ElasticSearch.
        """
        writers = persons["writer"].get(film_work["id"])
        actors = persons["actor"].get(film_work["id"])
        directors = persons["director"].get(film_work["id"])
        return {
            'id': film_work['id'],
            'genre': ', '.join([i_genre["name"] for i_genre in film_genres]),
            'writers': writers,
            'actors': actors,
            # Склеиваем через запятую имена
            'actors_names': ', '.join([actor['name'] for actor in actors]) if actors else None,
            'writers_names': ', '.join([writer['name'] for writer in writers]) if writers else None,
            'imdb_rating': float(film_work['rating']) if film_work['rating'] else None,
            'title': film_work['title'],
            'director': ', '.join([director["name"] for director in directors]) if directors else None,
            'description': film_work['description'] if film_work['description'] != 'N/A' else None
        }


def main():
    """
    Основная функция для запуска скрипта.
    """
    es_loader = ESLoader(url='https://127.0.0.1:9200')
    with conn_context(db_path='db.sqlite') as db_connection:
        etl_obj = ETL(conn=db_connection, es_loader=es_loader)
        etl_obj.load(index_name='movies')


if __name__ == "__main__":
    main()