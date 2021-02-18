from src.crawler.base_crawler import BaseCrawler
from bs4 import BeautifulSoup
import pandas as pd
from pprint import pprint
import re
import json
import os
import requests
from PIL import Image
import requests
import copy
import torchvision.transforms as transforms
from torchvision.transforms import ToTensor,Grayscale,ToPILImage
"""
#時系列review
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException,StaleElementReferenceException
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
import datetime
"""

def one_to_three(img):
    if img.shape[0]!=3:
        transform=transforms.Compose(
            [
                transforms.ToPILImage(),
                Grayscale(num_output_channels=3)
            ]
        )
        return transform(img)
    else:
        transform=transforms.ToPILImage()
        return transform(img)

transform=transforms.Compose(
                    [
                        transforms.ToTensor(),
                        transforms.Lambda(one_to_three),
                        transforms.Resize([300,256]),
                        transforms.ToTensor()
                    ])

#データ保存用ディレクトリ
di="data/Feature_data"
idi="data/image"
jidi="data/jimage"

#時系列review
firefox='/home/kwk/share20A/dentsu/Sugawara/example/fire/usr/bin/firefox'
driver=r'/home/kwk/share/dentsu/Hisamitsu/lib/geckodriver'

def getHTMLText(url):
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.text


def load_image(file_name,option="eng",idi=idi,jidi=jidi):
    if option == "jap":
        idi = jidi
    elif option != "eng":
        print("please choose eng or jap") 
        raise Exception
   
    try:
        im = Image.open(idi+"/"+file_name+".jpg")
        image= transform(im)
        
        im.close()
        return image
    except Exception as e:
        print(e,"please load image of "+file_name)

class Feature_Crawler(BaseCrawler):
    def __init__(self,start=0,last=0,order_list=None,order_name=None,info=[],movie_only=True,di=di,idi=idi,jidi=jidi,firefox=firefox,driver=driver):
        super().__init__()
        self.idi=idi
        self.jidi=jidi
        self.di=di
        self.firefox=firefox
        self.driver=driver
        self.start=start
        self.last=last
        self.order_list=order_list
        self.order_name=order_name
        self.movie_only=movie_only
        
        #欲しい特徴の『名前』1 こちらは『処理』と順を揃える
        self.feature_list=["titleId",'image_exist','j_image_exist','story_line','domestic_money','international_money','full_money']
        #他['domestic_rate','international_rate',"dates","stars","reviews"]
        
        # 特徴の『名前』2 順不同
        self.all_feature=[  'contentRating',  'description','datePublished', 'keywords']
        #'name','review',@context', '@type', 'url','image','actor','director','creator','genre','duration'
        
        # 特徴の『名前』3 順不同
        self.detail_feature=['aggregateRating']
        self.feature_dict={'aggregateRating':['ratingCount','bestRating','worstRating','ratingValue']}
        #'trailer'
        

        
        
        self.feature_list.extend(self.all_feature)
        for f in self.detail_feature:
            self.feature_list.extend(self.feature_dict[f])
        self.load_csv()
        

    def get_feature(self, movie_id):
        try:
            os.makedirs(self.idi)
        except FileExistsError:
            pass
        try:
            os.makedirs(self.jidi)
        except FileExistsError:
            pass
        res = self.get_response('https://www.imdb.com/title/' + movie_id + '/')
        soup = BeautifulSoup(markup=res.content, features='html.parser')
        try:
            json_data = soup.find('script', {'type' : 'application/ld+json'})
        except Exception as e:
            return None
        parsed_json = json.loads(json_data.contents[0])
        kind = soup.find("a",href="/search/title?genres=movie&explore=title_type,genres")

        if parsed_json["@type"]=="Movie" and self.movie_only:
            
            #欲しい特徴を得る『処理』1(名前と順を揃える)
            features=[]

            
            #画像
            img_url = parsed_json.get('image')
            try:
                res_img = self.get_response(img_url)
                self.save_image(movie_id,res_img)
            except Exception as e:
                img_url=None
            features.append(img_url)
            
            
            #日本画像
            try:
                j_im="exist"
                self.j_getPoster(soup.find("title").text.split("(")[0],movie_id)
            except Exception as e:
                j_im=None
            if not os.path.exists(self.jidi+"/"+movie_id+".jpg"):
                j_im=None
            features.append(j_im)
            
            

            #story
            story_data=soup.find("div",{'class':"inline canwrap"})
            if story_data is None:
                story=None
            else:
                story=str(story_data.getText())[6:-2]
            features.append(story)
            
            #money
            try:
                box_url = "https://www.boxofficemojo.com/title/"+movie_id
                box_html = getHTMLText(box_url)
                box_soup = BeautifulSoup(box_html, 'html.parser')
                percent = box_soup.find_all('span', attrs = {'class':'percent'})
                money = box_soup.find_all('span', attrs = {'class':'money'})
                p = [pe.text for pe in percent]
                m = [float(mo.text.replace(",","")[1:]) for mo in money]
                if m[0]==m[1] and (p[0]=='–' or p[1]=='–'):
                    if p[0]=='–':
                        result = [None,m[0],m[1]]
                    elif p[1]=='–':
                        result = [m[0],None,m[1]]
                else:
                    result = [m[0],m[1],m[2]]
                #money rate
                #features.extend(p)
                features.extend(result)
            except Exception as e:
                #features.extend([None,None])
                features.extend([None,None,None])
            """"
            #時系列review
            
            try:
                features.extend(self.get_reviews(movie_id))
            except Exception as e:
                
                features.extend([None]*3)
            """
            
            #『処理』2 順不同
            for name in self.all_feature:
                try:
                    features.append(parsed_json[name])
                except Exception as e:
                    features.append(None)
                    
            #『処理』3 順不同
            for name in self.detail_feature:
                try:
                    detail=eval(str(parsed_json[name]))
                    for d_name in self.feature_dict[name]:
                        features.append(float(detail[d_name]))
                except Exception as e:
                    features.extend([None]*len(self.feature_dict[name]))
           
            return features
        else:
            return None
        
        
        
    def get_reviews(self, movie_id):
        try:
            os.makedirs(self.di)
        except FileExistsError:
            pass
        options = Options()
        options.headless = True
        options.binary=FirefoxBinary(self.firefox)
        driver = webdriver.Firefox(executable_path=self.driver,options=options)
        driver.get('https://www.imdb.com/title/' + movie_id + '/reviews')
        while True:
            try:
                showmore=WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH ,'//*[@id="load-more-trigger"]')))
                showmore.click()
            except TimeoutException:
                break
            except StaleElementReferenceException:
                break
        soup = BeautifulSoup(driver.page_source,'html.parser')
        dates=[]
        stars=[]
        reviews=[]

        for review_container in soup.find_all('div',{'class':'lister-item-content'}):
            #star
            try:
                star = review_container.find('span',{'class':'rating-other-user-rating'}).find('span').getText()
            except:
                star = "none"

            #review
            try:
                review = review_container.find('div',{'class':'text show-more__control'}).getText()
            except:
                review = "none"
            #date
            review_date = review_container.find('span',{'class':'review-date'}).getText()
            formated_date = datetime.datetime.strptime(review_date, '%d %B %Y').strftime('%Y/%m/%d') #YYYY/mm/dd HH:MM +0000に変換
            dates.append(formated_date)
            stars.append(star)
            reviews.append(reviews)

            #json_data = soup.find_all('div',{'class':'text show-more__control'})

            #print(json_data[0])
        return  [dates,stars,reviews]
    
    
    def j_getPoster(self, kw, imdbNo):
        root=self.jidi
        #キーワードを検索
        url = 'https://filmarks.com/search/movies?q=' + kw
        html = getHTMLText(url)
        soup = BeautifulSoup(html, 'html.parser')
        a = soup.find('a', attrs = {'class':'p-content-cassette__readmore'})
        url = 'https://filmarks.com/' + a['href']
        #映画の詳細ページを取得
        html = getHTMLText(url)
        soup = BeautifulSoup(html, 'html.parser')
        div = soup.find('div', attrs = {'class':'c-content c-content--large'})
        for i in div.descendants:
            if i.name == 'img':
                url = i['src']
                if url[-3:] == 'svg':
                    return
                break
        #ポスターのリンクから画像を保存
        r = requests.get(url)
        file = root + "/"+ imdbNo + '.jpg'
        with open(file, 'wb') as f:
            f.write(r.content)
            f.close()
    
    def save_image(self, file_name, res_img):
        if not os.path.exists(self.idi+"/"+file_name+".jpg"):
            try:
                with open(self.idi+"/"+file_name+".jpg", 'wb') as img_file:
                    img_file.write(res_img.content)
                print('Save image for ' + file_name)
            except Exception as e:
                print('Saving image failure for ' + file_name)   
            
    def movie_num(self,n):
        return "tt"+(7-len(str(n)))*"0"+str(n)
    
    @property
    def feature_di(self):
        if self.order_name is None:
            return self.di+"/"+"_s"+str(self.start)+"_l"+str(self.last)+".csv"
        #"_".join(self.feature_list)+
        else:
            return self.di+"/"+self.order_name+".csv"
    
    def save_as_csv(self):
        try:
            os.makedirs(self.di)
        except FileExistsError:
            pass
        pd_list=[]
        search=None
        if self.order_name is None:
            search=range(self.start,self.last+1,1)
        else:
            search=self.order_list
            
        for i in search:
            try:
                if self.order_name is None:
                    movie_id=self.movie_num(i)
                else:
                    movie_id=i
                feature=self.get_feature(movie_id)
                id_feature=[movie_id]
                if feature !=  None:
                    id_feature.extend(feature)
                    pd_list.append(id_feature)
            except Exception as e:
                print(e)
                continue
        pd_list=pd.DataFrame(pd_list,columns=self.feature_list)
        pd_list.to_csv(self.feature_di)
        
        return pd_list
    
    
    def load_csv(self):
        if not os.path.exists(self.feature_di):
            self.feature=self.save_as_csv()
        self.feature=pd.read_csv(self.feature_di,index_col=1)
        self.feature=self.feature.drop('Unnamed: 0',axis=1)   