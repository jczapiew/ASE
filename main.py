from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
import boto3
import html2text
from warcio.archiveiterator import ArchiveIterator
from warcio.bufferedreaders import BytesIO
from bs4 import BeautifulSoup
import datetime

interesting_words = ["wirus", "pandemia", "covid", "covid19", "biznes", "polska"]
word_count = [0] * len(interesting_words)

# ==============================================================
# ==================== Deklaracje funkcji ======================
# ==============================================================

# Funkcja zamieniająca rekord zmiennej dataframe na wpis w zmiennej dictionary
def process_warc_path_df(record):
    return {'url': record.url, 'warc_filename': record.warc_filename,
            'warc_record_offset': record.warc_record_offset, 'warc_record_length': record.warc_record_length}


# Funkcja odczytująca i przetwarzająca pliki WARC z Common Crawl(wersja testowa, do zmiany)
def fetch_process_warc_records(rows):
    global interesting_words, word_count
    print('>>>>>>>>>>>>>>>>>')
    s3client = boto3.client('s3')

    for row in rows:
        url = row['url']
        warc_path = row['warc_filename']
        offset = int(row['warc_record_offset'])
        length = int(row['warc_record_length'])
        rangereq = 'bytes={}-{}'.format(offset, (offset+length-1))
        response = s3client.get_object(Bucket='commoncrawl', Key=warc_path, Range=rangereq)

        record_stream = BytesIO(response["Body"].read())

        parser = html2text.HTML2Text()
        parser.ignore_links = True
        for record in ArchiveIterator(record_stream):
            date_str = record.rec_headers.get_header("WARC-Date").split("T")[0]
            date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
            print('Date: ', date_obj)
            page = record.content_stream().read()

            soup = BeautifulSoup(page, features="html.parser")

            # wyrzucenie elementów 'script' i 'style'
            for script in soup(["script", "style"]):
                script.extract()

            # zamiana na czysty tekst
            text = soup.get_text()
            # linijka po linijce bez białych znaków na końcach
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            # wyrzucenie pustych linii
            text = '\n'.join(chunk for chunk in chunks if chunk)

            # liczenie słów
            for word in text.split():
                if len(word) < 4:  # odrzucanie krotkich slow (np. "na")
                    continue

                word = word.lower()
                # jeśli słowo jest tym którego szukamy to ++
                if word in interesting_words:
                    i = interesting_words.index(word)
                    word_count[i] += 1

    print('>>>>>>>>>>>>>>>>>')


# ==============================================================
# ==================== Początek programu =======================
# ==============================================================

print("==============================================")
print("+--------------------------------------------+")
print("==============================================")

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

# Wybranie przedziału danych, które nas interesują, za pomocą odpowiedniego zapytania
query_txt = 'SELECT url, warc_filename, warc_record_offset, warc_record_length FROM ccindex ' \
            'WHERE crawl = "CC-MAIN-2020-16" AND subset = "warc" AND url_host_tld = "pl"'
sqlDF = sql_context.sql(query_txt).limit(100)
# sqlDF.show()

# Zamiana zmiennej typu dataframe, zawierającej linki do plików WARC, na zmienną typu dictionary
warc_paths_rdd = sqlDF.rdd.map(process_warc_path_df).collect()

# Odczytanie i przetwarzanie plików WARC
fetch_process_warc_records(warc_paths_rdd)

print(interesting_words)
print(word_count)

print("==============================================")
print("+--------------------------------------------+")
print("==============================================")
