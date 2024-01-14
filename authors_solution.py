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
        response = requests.post(
            urljoin(self.url, '_bulk'),
            data=str_query,
            headers={'Content-Type': 'application/x-ndjson'}
        )
        json_response = json.loads(response.content.decode())
        for item in json_response['items']:
            error_message = item['index'].get('error')
            if error_message:
                logger.error(error_message)


class ETL:
    # Хуета, а не запрос, не актуален
    SQL = '''
        /* Используем CTE для читаемости. Здесь нет прироста
        производительности, поэтому можно поменять на subquery */
        WITH x as (
        -- Используем group_concat, чтобы собрать id и имена
        всех актёров в один список после join'а с таблицей actors
        -- Отметим, что порядок id и имён совпадает
        -- Не стоит забывать про many-to-many связь между
        таблицами фильмов и актёров
        SELECT m.id, group_concat(a.id) as actors_ids, group_concat(a.name) as actors_names
        FROM movies m
        LEFT JOIN movie_actors ma on m.id = ma.movie_id
        LEFT JOIN actors a on ma.actor_id = a.id
        GROUP BY m.id
        )
        -- Получаем список всех фильмов со сценаристами и актёрами
        SELECT m.id, genre, director, title, plot, imdb_rating, x.actors_ids, x.actors_names,
        /* Этот CASE решает проблему в дизайне таблицы:
        если сценарист всего один, то он записан простой строкой
        в столбце writer и id. В противном случае данные
        хранятся в столбце writers и записаны в виде
        списка объектов JSON.
        Для исправления ситуации применяем хак:
        приводим одиночные записи сценаристов к списку
        из одного объекта JSON и кладём всё в поле writers */
        CASE
        WHEN m.writers = '' THEN '[{"id": "' || m.writer || '"}]'
        ELSE m.writers
        END AS writers
        FROM movies m
        LEFT JOIN x ON m.id = x.id
    '''
    # Нормальный запрос
    MY_SQL_QUERY = """
    
    """

    def __init__(self, conn: sqlite3.Connection, es_loader: ESLoader):
        self.es_loader = es_loader
        self.conn = conn

    def _transform_row(self, row: dict, writers: dict) -> dict:
        """
        Основная логика преобразования данных из SQLite во внутреннее
        представление, которое дальше будет уходить в Elasticsearch
        Решаемы проблемы:
        1) genre в БД указан в виде строки из одного или нескольких
        жанров, разделённых запятыми -> преобразовываем в список жанров.
        2) writers из запроса в БД приходит в виде списка словарей id'шников
        -> обогащаем именами из полученных заранее сценаристов
        и добавляем к каждому id ещё и имя
        3) actors формируем из двух полей запроса (actors_ids и actors_names)
        в список словарей, наподобие списка сценаристов.
        4) для полей writers, imdb_rating, director и description меняем
        поля 'N/A' на None.
        :param row: строка из БД
        :param writers: текущие сценаристы
        :return: подходящая строка для сохранения в Elasticsearch
        """
        movie_writers = []
        writers_set = set()
        for writer in json.loads(row['writers']):
            writer_id = writer['id']
            if writers[writer_id]['name'] != 'N/A' and writer_id not in writers_set:
                movie_writers.append(writers[writer_id])
                writers_set.add(writer_id)
        actors = []
        if row['actors_ids'] is not None and row['actors_names'] is not None:
            actors = [
                {'id': _id, 'name': name}
                for _id, name in zip(row['actors_ids'].split(','), row['actors_names'].split(','))
                if name != 'N/A'
            ]
        actors_names = [x for x in row['actors_names'].split(',') if x != 'N/A']
        return {
            'id': row['id'],
            'genre': row['genre'].replace(' ', '').split(','),
            'writers': movie_writers,
            'actors': actors,
            'actors_names': actors_names,
            'writers_names': [x['name'] for x in movie_writers],
            'imdb_rating': float(row['imdb_rating']) if row['imdb_rating'] != 'N/A' else None,
            'title': row['title'],
            'director': [x.strip() for x in row['director'].split(',')] if row['director'] != 'N/A' else None,
            'description': row['plot'] if row['plot'] != 'N/A' else None
        }

    def load_writers_names(self) -> dict:
        """
        Получаем ID и имена всех person с person.role = writer.
        {
        "writer_id": {"id": "writer_id", "name": "writer full_name"},
        ...
        }
        """
        writers = {}
        sql_query = """
        SELECT person_film_work.person_id as id, person.full_name as name FROM person_film_work 
        INNER JOIN person ON person.id = person_film_work.person_id 
        WHERE person_film_work.role = 'writer';
        """
        for writer in self.conn.execute(sql_query):
            writers[writer['id']] = writer
        return writers

    def load_actors_ids_and_names(self) -> dict:
        """
        Получаем ID и имена всех person с person.role = actor.
        {
        "actor_id": {"id": "actor_id", "name": "actor full_name"},
        ...
        }
        """
        actors = {}
        sql_query = """
        SELECT person_film_work.person_id as id, person.full_name as name FROM person_film_work 
        INNER JOIN person ON person.id = person_film_work.person_id 
        WHERE person_film_work.role = 'actor';
        """
        for actor in self.conn.execute(sql_query):
            actors[actor['id']] = actor
        return actors

    def load_directors_names(self) -> dict:
        """
        Получаем имена всех person с person.role = director.
        {
        "director_id": {"id": "director_id", "name": "director full_name"},
        ...
        }
        """
        directors = {}
        sql_query = """
        SELECT person_film_work.person_id as id, person.full_name as name FROM person_film_work 
        INNER JOIN person ON person.id = person_film_work.person_id 
        WHERE person_film_work.role = 'director';
        """
        for director in self.conn.execute(sql_query):
            directors[director['id']] = director
        return directors

    def load_genres_names(self):
        """
        Получаем название жанров и кладем их в словарь, в котором ключом будет film_work_id.
        Так будет удобнее по фильму достать инфу о жанре.
        {
        "film_work_id": {"id": "genre_id", "name": "genre name"},
        ...
        }
        """
        genres = {}
        sql_query = """
        SELECT genre.id, genre.name, genre_film_work.film_work_id FROM genre
        INNER JOIN genre_film_work ON genre.id = genre_film_work.genre_id
        GROUP BY genre_film_work.film_work_id;
        """
        for genre in self.conn.execute(sql_query):
            genres[genre['id']] = genre
        return genres

    def load(self, index_name: str):
        """
        Основной метод для вашего ETL.
        Обязательно используйте метод load_to_es — это будет проверяться
        :param index_name: название индекса, в который будут грузиться данные
        """
        records = []
        writers = self.load_writers_names()
        actors = self.load_actors_ids_and_names()
        directors = self.load_directors_names()
        genres = self.load_genres_names()
        # TODO: дописать применение actors directors genres.
        #  Далее нужно доставать фильмы и упаковывать для них записи ES
        for row in self.conn.execute(self.SQL):
            transformed_row = self._transform_row(row, writers)
            records.append(transformed_row)
        self.es_loader.load_to_es(records, index_name)


def main():
    """
    Основная функция для запуска скрипта.
    """
    es_loader = ESLoader(url='http://127.0.0.1:9200')
    with conn_context(db_path='db.sqlite') as db_connection:
        etl_obj = ETL(conn=db_connection, es_loader=es_loader)
        etl_obj.load(index_name='movies')


if __name__ == "__main__":
    main()