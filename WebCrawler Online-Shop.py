from urllib import request, error
from bs4 import BeautifulSoup
from blitzdb import Document
from blitzdb import FileBackend
import csv

articles_for_processing = []
with open('Einspieldaten_new.csv', 'r', encoding='utf-8') as csvDataFile:
    csvReader = csv.reader(csvDataFile, delimiter=';')
    header = None
    for i, row in enumerate(csvReader):
        if i == 0:
            header = row
        else:
            dic = {}
            for key in header:
                dic[key] = row.pop(0)
            articles_for_processing.append(dic)

backend = FileBackend("./db_data_XXXX")

class Article(Document):
    pass

for article in articles_for_processing:
    url = 'https://www.heizungsdiscount24.de/shop/system/?func=detailcall&artnr=Wolf-{}'\
        .format(article['bestellnummer'])
    try:
        html = request.urlopen(url)
        xml_syntax = BeautifulSoup(html.read(), "lxml")
        rv_article_head = xml_syntax.find("h1")
        if rv_article_head:
            rv_article_price = xml_syntax.find("span", {"class": {"os_detail_price"}}).text
            rv_article_price = rv_article_price.replace('EUR', '').replace('\xa0', '')
            rv_article_delivery_time = xml_syntax.find("div", {"id": {"detailtmpdel"}})
            if rv_article_price and rv_article_delivery_time:
                delivery_time = ''
                for deli in rv_article_delivery_time:
                    delivery_time += deli.text
                    break
                article['hd24_vk_brutto'] = rv_article_price
                article['hd24_lieferzeit'] = delivery_time
                rv_article_as_object = Article(article)
                backend.save(rv_article_as_object)
                backend.commit()
                print(article)

        else:
            print('Artikel {} ist bei HD24 nicht gelistet.'.format(article['bestellnummer']))
            article['hd24_vk_brutto'] = ''
            article['hd24_lieferzeit'] = ''

    except error.URLError as ERROR:
        print(ERROR)


filename = "Ausgabedatei.csv"
field_names = ['artikelnummer', 'bestellnummer', 'uvp', 'rabatt',
               'ek_netto', 'vk_netto', 'zuschlagskalkulation', 'kurzbezeichnung', 'lagerfaehig', 'lieferstatus',
               'hd24_vk_brutto', 'hd24_lieferzeit', 'pk']

with open(filename, 'w') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=field_names, delimiter=';')
    writer.writeheader()
    writer.writerows(articles_for_processing)
