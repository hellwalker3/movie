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
from torch.utils.data import DataLoader
import torchvision.transforms as transforms
from torchvision.transforms import ToTensor,Grayscale,ToPILImage
import numpy as np
import functools as ft
import itertools as it
import matplotlib.pyplot as plt

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
    def __init__(self,start=0,last=0,order_list=None,order_name=None,tsv=False,optional_data=False,nessesary_col=[],target_col='full_money',n_splits=5,shuffle=True):
        self.nessesary_col=nessesary_col
        self.target_col=target_col
        self.fea=Feature_Crawler(start,last,order_list,order_name)
        #TSV データを使う用(時間がかかる)
        if tsv:
            self.tsv=Tsv_Crawler()
            self.full=self.tsv.title.join(self.fea.feature,how="inner")
        else:
            self.full=self.fea.feature
            
        if optional_data is not None:
            self.full = self.full.join(optional_data,how="inner")
        self.elminate_not_have_nessesary()
        
            
        
        self._full = self.set_push_data(self.full)
        
        
        self.splitter = KFold(n_splits=n_splits, shuffle=shuffle)
        self.split_full=[[self.set_push_data(self.full.iloc[train]), self.set_push_data(self.full.iloc[test])]for train,test in self.splitter.split(self.full)]
        
    def elminate_not_have_nessesary(self):
        self.full=self.full.dropna(subset=self.nessesary_col)
        self.full=self.full.dropna(subset=[self.target_col])
        
    def set_push_data(self,panda):
        class push_data():
            def __init__(self,panda=panda,target=self.target_col, eng_poster=False, w_poster=False):
                self.full=panda
                self.full
                self.target=self.full[target]
                self.explain=self.full.drop(columns=target)
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
                
                #英語ポスターと日本語ポスター
                if self.w_poster:
                    print(self.full_title[idx])
                    poster=transform(self.image_loader(self.full_title[idx]))
                    j_poster=transform(self.j_image_loader(self.full_title[idx]))
                    #print(self.explain.iloc[idx])
                    return (poster,j_poster) ,self.target.iloc[idx]
                
                
                #英ポスター
                if self.title_size:
                    poster=transform(self.image_loader(self.full_title[idx]))
                    #他のデータも(ex　title,size)
                    return poster, self.target.iloc[idx]
                
                
                return  self.target.iloc[idx]
                    
        return push_data
            
