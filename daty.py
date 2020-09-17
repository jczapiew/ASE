import datetime

dt = datetime.datetime(2019, 3, 1)
end = datetime.datetime(2019, 4, 30, 23, 59, 59)
step = datetime.timedelta(days=1)

result2019 = []

while dt < end:
    result2019.append(dt.strftime('%Y-%m-%d'))
    dt += step

print(result2019)

dt = datetime.datetime(2020, 3, 1)
end = datetime.datetime(2020, 4, 30, 23, 59, 59)
step = datetime.timedelta(days=1)

result2020 = []

while dt < end:
    result2020.append(dt.strftime('%Y-%m-%d'))
    dt += step

print(result2020)