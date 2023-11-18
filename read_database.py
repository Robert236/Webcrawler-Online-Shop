from blitzdb import Document
from blitzdb import FileBackend
import csv

backend = FileBackend("./db_data_XXXX")


class Article(Document):
    pass


articles = backend.filter(Article, {'zuschlagskalkulation': '13 XXXXX +13%'})
print("Found %d all_articles" % len(articles))

field_names = ['artikelnummer', 'bestellnummer', 'uvp', 'rabatt',
               'ek_netto', 'vk_netto', 'zuschlagskalkulation', 'kurzbezeichnung', 'lagerfaehig', 'lieferstatus',
               'hd24_vk_brutto', 'hd24_lieferzeit']

filtered_articles = []
for article in articles:
    dic = {}
    for key in field_names:
        if key == 'artikelnummer':
            dic[key] = article.artikelnummer
        elif key == 'bestellnummer':
            dic[key] = article.bestellnummer
        elif key == 'uvp':
            dic[key] = article.uvp
        elif key == 'rabatt':
            dic[key] = article.rabatt
        elif key == 'ek_netto':
            dic[key] = article.ek_netto
        elif key == 'vk_netto':
            dic[key] = article.vk_netto
        elif key == 'zuschlagskalkulation':
            dic[key] = article.zuschlagskalkulation
        elif key == 'kurzbezeichnung':
            dic[key] = article.kurzbezeichnung
        elif key == 'lagerfaehig':
            dic[key] = article.lagerfaehig
        elif key == 'lieferstatus':
            dic[key] = article.lieferstatus
        elif key == 'hd24_vk_brutto':
            dic[key] = article.hd24_vk_brutto
        elif key == 'hd24_lieferzeit':
            dic[key] = article.hd24_lieferzeit
    filtered_articles.append(dic)

for line in filtered_articles:
    print(line)

filename = "Daten aus der DB ausgespielt.csv"
with open(filename, 'w') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=field_names, delimiter=';')
    writer.writeheader()
    writer.writerows(filtered_articles)
