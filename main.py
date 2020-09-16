from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
import boto3
from warcio.archiveiterator import ArchiveIterator
from warcio.bufferedreaders import BytesIO
from bs4 import BeautifulSoup
import datetime
import re


# ==============================================================
# ==================== Deklaracje funkcji ======================
# ==============================================================

# Funkcja wczytująca i przetwarzająca pliki WARC z Common Crawl w celu zliczenia wystąpień słów kluczy
# w danym przedziale czasu.
# Funkcja zwraca klucz w postaci data + słowo klucz oraz ilość wystąpień danego słowa klucza
def process_warc_records(warc_records):
    s3_client = boto3.client('s3')

    for record in warc_records:
        warc_path = record['warc_filename']
        warc_offset = int(record['warc_record_offset'])
        warc_length = int(record['warc_record_length'])
        warc_range = 'bytes={}-{}'.format(warc_offset, (warc_offset+warc_length-1))
        # Wczytywanie pliku WARC z Common Crawl
        response = s3_client.get_object(Bucket='commoncrawl', Key=warc_path, Range=warc_range)

        # Odczytywanie danych z pliku WARC
        warc_record_stream = BytesIO(response["Body"].read())

        # Przetwarzanie rekordów pliku WARC
        for warc_record in ArchiveIterator(warc_record_stream):
            # Odczytywanie daty pliku WARC
            date_str = warc_record.rec_headers.get_header("WARC-Date").split("T")[0]
            date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
            # Czytanie danych zapisanych w postaci HTML
            page = warc_record.content_stream().read()
            soup = BeautifulSoup(page, features="html.parser")
            # wyrzucenie elementów 'script' i 'style'
            for script in soup(["script", "style"]):
                script.extract()
            # zamiana na czysty tekst
            page_text = soup.get_text().lower()

            # Zliczanie wystąpień każdego słowa klucza w tekście
            for word in word_patterns:
                count = sum(1 for _ in re.finditer(r'\b%s\b' % re.escape(word), page_text))
                key = str(date_obj) + '/' + word  # Klucz = data pliku WARC + słowo klucz
                yield key, count



# ==============================================================
# ==================== Początek programu =======================
# ==============================================================

print("==============================================")
print("+--------------------------------------------+")
print("==============================================")

# Słowa klucze
word_patterns = ['covid', 'covid19', 'pandemia', 'wirus']

# Inicializacja Sparka
conf = (SparkConf()
        .setAppName("Ase")
        .set("spark.driver.maxResultSize", "2g"))

sc = SparkContext(conf=conf)
sql_context = SQLContext(sc)
spark = SparkSession.builder.getOrCreate()

# Wczytanie danych z bucketa Common Crawl
input_bucket = 's3://commoncrawl/cc-index/table/cc-main/warc/'
df = sql_context.read.load(input_bucket)
df.createOrReplaceTempView('ccindex')

# Użycie odpowiedniego zapytania SQL w celu stworzenia DataFrame, który zawiera spis plików WARC
# z interesującego nas przedziału danych
query_txt = 'SELECT warc_filename, warc_record_offset, warc_record_length FROM ccindex ' \
            'WHERE (crawl = "CC-MAIN-2019-13" OR crawl = "CC-MAIN-2019-18" OR crawl = "CC-MAIN-2020-16") ' \
            'AND subset = "warc" AND url_host_tld = "pl"'
sqlDF = sql_context.sql(query_txt)
# sqlDF.show()

# Odczytanie i przetwarzanie plików WARC(zliuczanie wystąpień słów kluczy w danym dniu)
word_counts = sqlDF.rdd.mapPartitions(process_warc_records).reduceByKey(lambda a, b: a + b).collect()

# Sparsowanie wyników
word_counts_array = []
for row in word_counts:
    word_key = row[0].split('/')
    date = word_key[0]
    word = word_key[1]
    count = int(row[1])
    word_counts_array.append([date, word, count])
# Sortowanie wyników względem daty
word_counts_sorted = sorted(word_counts_array, key=lambda word_row: word_row[0])

# Stworzenie finalnej tablicy z danymi w celu łatwiejszej ich wizualizacji i przetwarzania(rządy to daty, kolumny to słowa klucze)
word_counts_final = []
index_row = ['DATE']
for word in word_patterns:
    index_row.append(word)

word_counts_final.append(index_row)
temp_row = [word_counts_sorted[0][0]]
for i in range(len(word_patterns)):
    temp_row.append(0)
word_counts_final.append(temp_row)

for row in word_counts_sorted:
    items = len(word_counts_final)
    date = row[0]
    word = row[1]
    count = row[2]
    if word_counts_final[items - 1][0] != date:
        new_row = [date]
        for i in range(len(word_patterns)):
            new_row.append(0)
        word_counts_final.append(new_row)
        items = len(word_counts_final)
    for i in range(1, len(index_row), 1):
        key_word = index_row[i]
        if word == key_word:
            word_counts_final[items - 1][i] = count
            break

# Zamiana finalnej tabeli na DataFrame i zapisanie danych do pliku .csv
df_words = sc.parallelize(word_counts_final).toDF(index_row)
df_words.repartition(1).write.csv('s3://aseeee/dane.csv')
df_words.show()


print("==============================================")
print("+--------------------------------------------+")
print("==============================================")
