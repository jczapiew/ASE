from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
import boto3
import html2text
from warcio.archiveiterator import ArchiveIterator
from warcio.bufferedreaders import BytesIO


# ==============================================================
# ==================== Deklaracje funkcji ======================
# ==============================================================


# Funkcja zamieniająca rekord zmiennej dataframe na wpis w zmiennej dictionary
def process_warc_path_df(record):
    return {'url': record.url, 'warc_filename': record.warc_filename,
            'warc_record_offset': record.warc_record_offset, 'warc_record_length': record.warc_record_length}


# Funkcja odczytująca i przetwarzająca pliki WARC z Common Crawl(wersja testowa, do zmiany)
def fetch_process_warc_records(rows):
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
            page = record.content_stream().read()
            #text = parser.handle(page)
            print(page)
            # words = map(lambda w: w.lower(), word_pattern.findall(text))
            # for word in words:
            #     yield word, 1
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
sqlDF = sql_context.sql(query_txt).limit(10)
# sqlDF.show()

# Zamiana zmiennej typu dataframe, zawierającej linki do plików WARC, na zmienną typu dictionary
warc_paths_rdd = sqlDF.rdd.map(process_warc_path_df).collect()

# Odczytanie i przetwarzanie plików WARC
fetch_process_warc_records(warc_paths_rdd)


print("==============================================")
print("+--------------------------------------------+")
print("==============================================")
