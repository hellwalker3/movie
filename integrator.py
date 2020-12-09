from src.crawler.base_crawler import BaseCrawler
from bs4 import BeautifulSoup
import pandas as pd
from pprint import pprint
import re
import json
import os
from Feature_Crawler import Feature_Crawler, load_image
from Tsv_Crawler import Tsv_Crawler
from sklearn.model_selection import KFold, train_test_split
import torchvision.transforms as transforms
from torchvision.transforms import ToTensor,Grayscale,ToPILImage
import numpy as np
import functools as ft
import itertools as it
import matplotlib.pyplot as plt
from sklearn.preprocessing import MultiLabelBinarizer
import gensim

def one_to_three(img):
    if len(img.shape)==2:
        transform=transforms.Compose(
            [
                transforms.ToPILimage(),
                Grayscale(num_output_channels=3)
            ]
        )
        return transform(img)
    else:
        transform=transforms.ToPILImage()
        return transform(img)

class integrator():
    def __init__(self,start=0,last=0,order_list=None,order_name=None,movie_only=True,tsv=False,optional_data=False,not_none_col=[],target_col=None,delete_col=[],n_splits=5,shuffle=True):
        self.delete_col=delete_col
        #不要もしくは重なるcolumn('averageRating', 'numVotes'重複, 'attributes','job'扱う方法が不明)
        self.delete_col.extend(['title','primaryTitle', 'originalTitle','characters','attributes','job','isAdult','averageRating', 'numVotes','image_exist','j_image_exist','description'])
        
        self.first_number_col=['actor','director','writers','directors','creator','nconst','parentTconst','startYear','endYear','runtimeMinutes','seasonNumber','episodeNumber','isOriginalTitle']
        
        #カテゴリー onehot
        self.categorical_col=['region', 'language', 'types', 'titleType','category','contentRating']
        
        self.date=['datePublished']
        self.genre=['genres']
        
        self.word=['keywords','story_line']
        
        
        self.not_none_col=not_none_col
        self.target_col=target_col
        self.fea=Feature_Crawler(start,last,order_list,order_name,movie_only)
        #TSV データを使う用(時間がかかる)
        if tsv:
            self.tsv=Tsv_Crawler()
            self.full=self.tsv.title.join(self.fea.feature,how="inner")
        else:
            self.full=self.fea.feature
            
        if optional_data is not None:
            self.full = self.full.join(optional_data,how="inner")
        self.elminate_not_have_nessesary()
        self.preprocess()
        
        self._full = self.set_push_data(self.full)
        self.splitter = KFold(n_splits=n_splits, shuffle=shuffle)
        self.split_full=[[self.set_push_data(self.full.iloc[train]), self.set_push_data(self.full.iloc[test])]for train,test in self.splitter.split(self.full)]
       
    def preprocess(self):
        
        for d in self.delete_col:
            try:
                self.full = self.full.drop(columns=d)
            except Exception as e:
                print(e,"self.full don't have "+d)
                
                
        for p in self.first_number_col:
            try:
                nums = self.full[p].str.split(',', expand =True)
                nums.columns = ['{}_{}'.format(p, i)  for i in range(len(nums.columns))]
                self.full = self.full.drop(p,axis=1)
                
                split=3 #3つまで
                for i,n in enumerate(nums.columns):
                    if i>split:
                        break
                    nums[n]=nums[n].astype(str).str.extract(r'(\d+)').astype(float)
                    nums[n].name=nums.columns[i]
                    self.full=pd.concat((self.full,nums[n]),axis=1)
                #self.full[p]=nums.astype(str).str.extract(r'(\d+)').astype(float)
            except Exception as e:
                print(e,"self.full don't have "+p)
        
                
        
        for g in self.genre:
            try:
                mlb = MultiLabelBinarizer()
                self.full[g]=self.full[g].map(lambda x: x.split(",")) 
                genres = pd.DataFrame(mlb.fit_transform(self.full[g]),columns=mlb.classes_)
                try:
                    genres = genres.drop('\\N',axis=1)
                except Exception as e:
                    print(e,"genres don't have \\N")
                genres.index = self.full.index
                #name
                genres.columns = ['{}_{}'.format(g, i)  for i in range(len(genres.columns))]
                self.full = self.full.drop(g,axis=1)
                self.full=pd.concat((self.full,genres),axis=1)
            except Exception as e:
                print(e,"self.full don't have "+g)
        
        for k in self.categorical_col:
            try:
                series = self.full[k]
                buf = pd.get_dummies(series)
                buf.loc[series.isna()] = None
                #name
                buf.columns = ['{}_{}'.format(k, i)  for i in range(len(buf.columns))]
                self.full = self.full.drop(k, axis=1)
                self.full = pd.concat([self.full, buf], axis=1)
            except Exception as e:
                 print(e,"self.full don't have "+k)
               
        self.full = self.full.replace('\\N',np.nan)
        
        for d in self.date:
            try:
                date = self.full[d].str.split('-', expand =True).astype(float)
                date.columns=['year','month','day']
                date = date.drop('year',axis=1)
                self.full = self.full.drop(d,axis=1)
                self.full=pd.concat((self.full,date),axis=1)
            except Exception as e:
                print(e,"self.full don't have "+d)
        

    def elminate_not_have_nessesary(self):
        self.full=self.full.dropna(subset=self.not_none_col)
        if self.target_col is not None:
            self.full=self.full.dropna(subset=[self.target_col])
        
    def set_push_data(self,panda):
        class push_data():
            def __init__(self,panda=panda,target=self.target_col,word=True,words=self.word, eng_poster=False, w_poster=False):
                self.full=panda
                self.full
                self.tar=target
                if target is not None:
                    self.target=self.full[target]
                    self.explain=self.full.drop(columns=target)
                self.wor=word
                if word:
                    self.word = self.explain[words]
                    self.explain=self.explain.drop(words,axis=1)
                    
                self.eng_poster=eng_poster
                self.w_poster=w_poster
                self.full_title=list(self.full.index)
                self.image_loader=load_image
                self.j_image_loader=ft.partial(load_image, option="jap")

            def __len__(self):
                return len(self.full)

            def __getitem__(self,idx):
                transform = transforms.Compose(
                    [
                        transforms.ToTensor(),
                        transforms.Lambda(one_to_three),
                        transforms.Resize([300,256]),
                        transforms.ToTensor()
                    ]
                )
                if self.wor:
                    if self.tar is not None:
                        #英語ポスターと日本語ポスター
                        if self.w_poster:
                            print(self.full_title[idx])
                            poster=transform(self.image_loader(self.full_title[idx]))
                            j_poster=transform(self.j_image_loader(self.full_title[idx]))
                            #print(self.explain.iloc[idx])
                            return (poster,j_poster) ,self.word.iloc[idx].values.tolist(), self.explain.iloc[idx].to_numpy().astype('float64'),self.target.iloc[idx]


                        #英ポスター
                        if self.eng_poster:
                            poster=transform(self.image_loader(self.full_title[idx]))
                            #他のデータも(ex　title,size)
                            return poster,self.word.iloc[idx].values.tolist(), self.explain.iloc[idx].to_numpy().astype('float64'), self.target.iloc[idx]


                        return self.word.iloc[idx].values.tolist(), self.explain.iloc[idx].to_numpy().astype('float64'), self.target.iloc[idx]

                    else:
                        if self.w_poster:
                            print(self.full_title[idx])
                            poster=transform(self.image_loader(self.full_title[idx]))
                            j_poster=transform(self.j_image_loader(self.full_title[idx]))
                            #print(self.explain.iloc[idx])
                            return (poster,j_poster) ,self.word.iloc[idx].values.tolist(), self.explain.iloc[idx].to_numpy().astype('float64')


                        #英ポスター
                        if self.eng_poster:
                            poster=transform(self.image_loader(self.full_title[idx]))
                            #他のデータも(ex　title,size)
                            return poster,self.word.iloc[idx].values.tolist(), self.explain.iloc[idx].to_numpy().astype('float64')


                        return  self.word.iloc[idx].values.tolist(), self.explain.iloc[idx].to_numpy().astype('float64')
                else:
                    if self.tar is not None:
                        #英語ポスターと日本語ポスター
                        if self.w_poster:
                            print(self.full_title[idx])
                            poster=transform(self.image_loader(self.full_title[idx]))
                            j_poster=transform(self.j_image_loader(self.full_title[idx]))
                            #print(self.explain.iloc[idx])
                            return (poster,j_poster) , self.explain.iloc[idx].to_numpy().astype('float64'),self.target.iloc[idx]


                        #英ポスター
                        if self.eng_poster:
                            poster=transform(self.image_loader(self.full_title[idx]))
                            #他のデータも(ex　title,size)
                            return poster, self.explain.iloc[idx].to_numpy().astype('float64'), self.target.iloc[idx]


                        return self.word.iloc[idx].values.tolist(), self.explain.iloc[idx].to_numpy().astype('float64'), self.target.iloc[idx]

                    else:
                        if self.w_poster:
                            print(self.full_title[idx])
                            poster=transform(self.image_loader(self.full_title[idx]))
                            j_poster=transform(self.j_image_loader(self.full_title[idx]))
                            #print(self.explain.iloc[idx])
                            return (poster,j_poster) , self.explain.iloc[idx].to_numpy().astype('float64')


                        #英ポスター
                        if self.eng_poster:
                            poster=transform(self.image_loader(self.full_title[idx]))
                            #他のデータも(ex　title,size)
                            return poster, self.explain.iloc[idx].to_numpy().astype('float64')


                        return  self.word.iloc[idx].values.tolist(), self.explain.iloc[idx].to_numpy().astype('float64')
               
                    
                    
        return push_data
            
