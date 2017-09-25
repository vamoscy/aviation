import csv
import pandas as pd
import glob
import os

files=glob.glob("/Users/michaelcy/Desktop/Course Profile/Andrew/untitled/raw data/*.csv")

for file in files:
    df = pd.read_csv(file)
    df=df.drop('Flight Number', axis=1)
    df.rename(columns={"Published Carrier": "Carrier Code"},inplace=True)
    df=df.groupby(['Carrier Code','Origin','Destination','Time series']).agg({'Seats':'sum','Frequency':'sum','Elapsed Time':'mean'})
    df=df.round({'Elapsed Time': 0})
    df.to_csv('out.csv')


    dfmkt=pd.read_csv('out.csv')
    dfmkt=dfmkt.drop(['Seats','Elapsed Time'],axis=1)
    dfmkt=dfmkt.sort_values(['Time series','Origin','Destination','Carrier Code'])
    dfmkt=dfmkt.set_index(['Time series','Origin','Destination','Carrier Code'])
    dfmkt= dfmkt.groupby(['Time series','Origin','Destination']).apply(lambda x: x / (x.sum()))
    dfmkt=dfmkt.round({'Frequency': 2})
    dfmkt.rename(columns={"Frequency": "Market Share"},inplace=True)
    dfmkt.to_csv('out2.csv')

    a = pd.read_csv("out.csv")
    b = pd.read_csv("out2.csv")
    b = b.dropna(axis=1)
    merged = a.merge(b, on=['Time series','Carrier Code','Origin','Destination'])
    merged.to_csv("out3.csv", index=False)


    with open('out3.csv', 'r',newline='') as fin:
        with open('out4.csv', 'w') as fout:
            writer = csv.writer(fout)
            for row in csv.reader(fin):
                if row[0]=='Carrier Code':
                    writer.writerow(row+['Year','Month'])
                else:
                    date=row[3].split('-')[:2]
                    writer.writerow(row+date)

    a=pd.read_csv("out4.csv")
    a=a[['Carrier Code','Time series','Year','Month','Origin','Destination','Elapsed Time','Seats','Frequency','Market Share']]
    a.to_csv("/Users/michaelcy/Desktop/Course Profile/Andrew/untitled/*.csv", index=False)
    os.rename("*.csv",file)
    print(file+' done')