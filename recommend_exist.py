import os
import http.server
import socketserver
import urllib.parse
from http import HTTPStatus
import pandas as pd
import gc


# coding: utf-8


df2023 = pd.read_csv('light_2023.csv')

mask = ['article_id', 'Товарные Категории']
articles = pd.DataFrame(df2023.groupby(mask).sum()).reset_index()[mask]

# Delete the DataFrame
del df2023
# Run garbage collection
gc.collect()

all_categories = set(articles['Товарные Категории'])
articles = articles.set_index('article_id')['Товарные Категории'].to_dict()

for key in list(
        articles.keys()):  # используем list() для получения списка ключей, чтобы избежать ошибок во время итерации
    cleaned_key = key.strip()
    if cleaned_key != key:
        articles[cleaned_key] = articles.pop(key)

recom_cstegories = pd.read_csv('updated_co_purchase.csv')
top_articles = pd.read_csv('top_articles_per_category.csv')

# List of terms to filter the rows
terms_russian = [
    "Переключатели окон", "Рычаги подвески", "Фиксатор резьбы",
    "Дверные панели", "Подшипники", "Стекло автомобильного окна",
    "Шаровые опоры", "Замки дверей", "Тяги рулевые"
]

# Check if any of the suggestions columns contains the terms
filtered_rows = recom_cstegories[recom_cstegories.apply(lambda row: row.isin(terms_russian).any(), axis=1)]
recom_cstegories = recom_cstegories[~recom_cstegories.apply(lambda row: row.isin(terms_russian).any(), axis=1)]
replaced_dict = [
    {'Selected Category': 'Сальники и уплотнительные кольца',
     'Suggestion 1': 'Моторное масло',
     'Suggestion 2': 'Трансмиссионное масло',
     'Suggestion 3': 'Прокладки и комплекты прокладок',
     'Suggestion 4': 'Масляный фильтр',
     'Suggestion 5': 'Подшипники трансмиссии'},
    {'Selected Category': 'Стеклоподъемник',
     'Suggestion 1': 'Дверные ручки',
     'Suggestion 2': 'Уплотнители для окон',
     'Suggestion 3': 'Боковые панели и ремчасти боковых панелей',
     'Suggestion 4': 'Стеклоподъемник',
     'Suggestion 5': 'Замок двери'},
    {'Selected Category': 'Стойка стабилизатора',
     'Suggestion 1': 'Амортизаторы',
     'Suggestion 2': 'Рычаги, комплекты рычагов и балки подвески',
     'Suggestion 3': 'Сайлентблоки',
     'Suggestion 4': 'Шаровая опора',
     'Suggestion 5': 'Рулевая тяга'},
    {'Selected Category': 'Болты и гайки',
     'Suggestion 1': 'Шайбы',
     'Suggestion 2': 'Сальники и уплотнительные кольца',
     'Suggestion 3': 'Ступицы, подшипники ступицы колеса и компоненты',
     'Suggestion 4': 'Хомуты',
     'Suggestion 5': 'Клипсы'}
]
replaced_rows_df = pd.DataFrame(replaced_dict)
recom_cstegories = pd.concat([recom_cstegories, replaced_rows_df])

recom_cstegories_dict = recom_cstegories.set_index('Selected Category').apply(lambda row: list(row), axis=1).to_dict()
top_articles_dict = top_articles.set_index('Category').apply(lambda row: list(row), axis=1).to_dict()

del replaced_rows_df, recom_cstegories
gc.collect()


def recommend_for(article):
    art_cat = articles[article]
    recommended_categories = recom_cstegories_dict[art_cat]
    answer = {"Recommendations for {} ({})".format(article, art_cat):
                  {_: top_articles_dict[_] for _ in recommended_categories}}
    return answer


class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):

        # Parse the URL and the query parameters
        parsed_path = urllib.parse.urlparse(self.path)
        query_params = urllib.parse.parse_qs(parsed_path.query)

        article = query_params.get('article', [''])[0]  # get the first 'article' parameter, default to ''

        # Construct a message
        if article:
            message = str(recommend_for(article))
        else:
            message = 'Please provide any article'

        # Send response
        self.send_response(HTTPStatus.OK)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')  # Specify UTF-8 encoding
        self.end_headers()
        self.wfile.write(message.encode('utf-8'))


port = int(os.getenv('PORT', 80))
print('Listening on port %s' % (port))
httpd = socketserver.TCPServer(('', port), Handler)
httpd.serve_forever()

# #
# # @app.get("/")
# # def read_root():
# #     return '''You're right there. Use the "recommend_for" method and pass your article.'''
# #
# #
# # @app.get("/recommend_for")
# # def recommend_for(article):
# #     art_cat = articles[article]
# #     recommended_categories = recom_cstegories_dict[art_cat]
# #     answer = {"Recommendations for {} ({})".format(article, art_cat):
# #                   {_: top_articles_dict[_] for _ in recommended_categories}}
# #     return answer
# #
# #
#
# #%%
# import os
# import http.server
# import socketserver
#
# from http import HTTPStatus
#
#
# class Handler(http.server.SimpleHTTPRequestHandler):
#     def do_GET(self):
#         self.send_response(HTTPStatus.OK)
#         self.end_headers()
#         msg = 'Hello! you requested %s' % (self.path)
#         self.wfile.write(msg.encode())
#
#
# port = int(os.getenv('PORT', 80))
# print('Listening on port %s' % (port))
# httpd = socketserver.TCPServer(('', port), Handler)
# httpd.serve_forever()