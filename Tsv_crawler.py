from src.crawler.base_crawler import BaseCrawler
from bs4 import BeautifulSoup
import pandas as pd
from pprint import pprint
import re
import json
import os
import requests
import gzip
import pandas as pd

class Tsv_Crawler(BaseCrawler):
    def __init__(self,last=-1,di="data/Tsv_data"):
        super().__init__()
        self.di=di
        self.file_name=["name.basics.tsv.gz","title.akas.tsv.gz","title.basics.tsv.gz","title.crew.tsv.gz","title.episode.tsv.gz","title.principals.tsv.gz","title.ratings.tsv.gz"]
        self.full_load(last)
        self.title_connect()
    
    def download_Tsv(self,url):      
        filename = url.split("/")[-1]
        with open(self.di+"/"+filename, "wb") as f:
            r = requests.get(url)
            f.write(r.content)
        print("success download from "+url)
        
    def full_download_Tsv(self):
        try:
            os.makedirs(self.di)
        except FileExistsError:
            pass
        
        test_url = 'https://datasets.imdbws.com'
        res = self.get_response(test_url)
        soup = BeautifulSoup(markup=res.content, features='html.parser')
        for tr in soup.find_all("a"):
            Tsv_url=tr.get('href')
            if re.search(".tsv.gz",Tsv_url):
                self.download_Tsv(Tsv_url)
    
                  
    def load_Tsv_as_pd(self,file):
        df=pd.read_table(self.di+"/"+file,index_col=0)
        #実行速度上げる用
        return df
    
    def elminate_double(self):
        self.full_pd[1]=self.full_pd[1][self.full_pd[1]["ordering"]==1]
        self.full_pd[5]=self.full_pd[5][self.full_pd[5]["ordering"]==1]
        self.full_pd[1]=self.full_pd[1].drop(columns='ordering')
        self.full_pd[5]=self.full_pd[5].drop(columns='ordering')
        
    
    def split_pd(self,last):
        for i in range(len(self.full_pd)):
            if i>=1:
                self.full_pd[i]=self.full_pd[i][self.full_pd[i].index.str[2:].astype(int)<=last]
            
    
    def full_load(self,last):
        self.full_pd=[]
        if not os.path.exists(self.di):
            self.full_download_Tsv()
        for name in self.file_name:
            self.full_pd.append(self.load_Tsv_as_pd(name))
        
        self.elminate_double()
        #self.split_pd(last)

                
    def load_title(self):
        self.title=pd.read_table(self.di+"/title.csv",index_col=0)

    
    def title_connect(self):
        c=0
        for pa in self.full_pd:
            c+=1
            if c==2:
                df=pa
            elif c>=3:
                df=df.join(pa ,how='outer')
        self.title=df
        self.title.to_csv(self.di+"/title.csv")
    

