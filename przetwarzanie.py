import pandas as pd
from matplotlib import pyplot as plt

df = pd.read_csv("dane_ase.csv")
print(df)

dates = df.iloc[:, 0].to_list()
covids = df['covid'].to_list()
covid19s = df['covid19'].to_list()
pandemias = df['pandemia'].to_list()
wiruses = df['wirus'].to_list()
bos = df['bo'].to_list()
ass = df['a'].to_list()

index = 0
for i, date in enumerate(dates):
    if "2020" in date:
        index = i
        break
print(index)
#
# for date in dates:
#     date = date + " 00:00:00"
#     print (date)


fig, axs = plt.subplots(2)
fig.tight_layout(pad=4)
fig.suptitle('Wystąpienia słów związanych z pandemią')
axs[0].set(xlabel='data', ylabel='liczba wystąpień')
axs[1].set(xlabel='data', ylabel='liczba wystąpień')
axs[0].plot(dates[0:index-1], bos[0:index-1], label='bo')
axs[0].plot(dates[0:index-1], ass[0:index-1], label='bo')
axs[1].plot(dates[index:len(dates)-1], bos[index:len(dates)-1], label='bo')
axs[1].plot(dates[index:len(dates)-1], ass[index:len(dates)-1], label='a')
axs[0].tick_params(labelrotation=30)
axs[1].tick_params(labelrotation=30)
axs[0].legend()
axs[1].legend()
# plt.xticks(rotation=30)
# fig.autofmt_xdate()
# axs[0].xaxis_date()
# fig.autofmt_xdate()
plt.show()

# plt.title("2019")
# plt.plot(dates[0:index-1], bos[0:index-1], label='bo')
# plt.plot(dates[0:index-1], bos[0:index-1], label='bo')
# plt.legend()
# plt.show()
#
# plt.title("2020")
# plt.plot(dates[index:len(dates)-1], bos[index:len(dates)-1], label='bo')
# plt.plot(dates[index:len(dates)-1], ass[index:len(dates)-1], label='a')
# plt.legend()
# plt.show()
