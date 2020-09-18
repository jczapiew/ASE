import pandas as pd
from matplotlib import pyplot as plt

fig, axs = plt.subplots(3, figsize=(7, 7))
fig.tight_layout(pad=4)
fig.suptitle('Wystąpienia słów związanych z pandemią')
axs[0].set(xlabel='data', ylabel='liczba wystąpień')
axs[1].set(xlabel='data', ylabel='liczba wystąpień')
axs[2].set(xlabel='data', ylabel='liczba wystąpień')


df = pd.read_csv("dane_2019_03.csv")
dates = df.iloc[:, 0].to_list()
covids = df['covid'].to_list()
covid19s = df['covid19'].to_list()
pandemias = df['pandemia'].to_list()
wiruses = df['wirus'].to_list()

axs[0].plot(dates, covids, label='covid')
axs[0].plot(dates, covid19s, label='covid19')
axs[0].plot(dates, pandemias, label='pandemia')
axs[0].plot(dates, wiruses, label='wirus')


df = pd.read_csv("dane_2019_04.csv")
dates = df.iloc[:, 0].to_list()
covids = df['covid'].to_list()
covid19s = df['covid19'].to_list()
pandemias = df['pandemia'].to_list()
wiruses = df['wirus'].to_list()

axs[1].plot(dates, covids, label='covid')
axs[1].plot(dates, covid19s, label='covid19')
axs[1].plot(dates, pandemias, label='pandemia')
axs[1].plot(dates, wiruses, label='wirus')


df = pd.read_csv("dane_2020_2pr.csv")
dates = df.iloc[:, 0].to_list()
covids = df['covid'].to_list()
covid19s = df['covid19'].to_list()
pandemias = df['pandemia'].to_list()
wiruses = df['wirus'].to_list()

axs[2].plot(dates, covids, label='covid')
axs[2].plot(dates, covid19s, label='covid19')
axs[2].plot(dates, pandemias, label='pandemia')
axs[2].plot(dates, wiruses, label='wirus')

axs[0].tick_params(labelrotation=30)
axs[1].tick_params(labelrotation=30)
axs[2].tick_params(labelrotation=30)
axs[0].legend()
axs[1].legend()
axs[2].legend()
plt.show()
