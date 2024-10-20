#libaries used
#pandas -> pip install pandas
#numpy -> pip install numpy
#sqlalchemy -> pip install SQLAlchemy
#python-dotenv -> pip install python-dotenv
#python connector -> pip install mysql-connector-python
import pandas as pd
import json
from itertools import count
from collections import defaultdict
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

#read the json and switch the rows and columns
pd.set_option('display.max_columns', None)
with open("games.json",encoding="utf8") as json_data:
    games = json.load(json_data)
json_data.close()
df = pd.DataFrame(games).transpose().dropna()

#set the columns to tuples instead of lists for easier operation
df['developers'] = df['developers'].apply(lambda x: tuple(x))
df['publishers'] = df['publishers'].apply(lambda x: tuple(x))
df['categories'] = df['categories'].apply(lambda x: tuple(x))
df['genres'] = df['genres'].apply(lambda x: tuple(x))
df['supported_languages'] = df['supported_languages'].apply(lambda x: tuple(x))
df['full_audio_languages'] = df['full_audio_languages'].apply(lambda x: tuple(x))

def maplist(df, mapping):
    result = []
    for element in df:
        result.append(mapping[tuple(element)])
    return result
    
#mapping
devmap = defaultdict(count().__next__)
pubmap = defaultdict(count().__next__)
catmap = defaultdict(count().__next__)
genmap = defaultdict(count().__next__)
audmap = defaultdict(count().__next__)
txtmap = defaultdict(count().__next__)
lngmap = defaultdict(count().__next__)
supmap = defaultdict(count().__next__)
tagmap = defaultdict(count().__next__)

#generate groupids
developer_groupid_result = maplist(df['developers'], devmap)
publisher_groupid_result = maplist(df['publishers'], pubmap)
category_groupid_result = maplist(df['categories'], catmap)
genre_groupid_result = maplist(df['genres'], genmap)
audio_language_groupid_result = maplist(df['full_audio_languages'], audmap)
supported_language_groupid_result = maplist(df['supported_languages'], txtmap)
supportgroup = np.array((df['windows'],df['mac'],df['linux'])).T
support_groupid_result = maplist(supportgroup, supmap)
languagegroup = np.array((audio_language_groupid_result,supported_language_groupid_result)).T
language_groupid_result = maplist(languagegroup, lngmap)
tags_groupid_result = maplist(df['tags'], tagmap)

#add group id into the columns for the dimension tables of developer, publisher, category, genre
df['developer_groupid'] = developer_groupid_result
df['publisher_groupid'] = publisher_groupid_result
df['category_groupid'] = category_groupid_result
df['genre_groupid'] = genre_groupid_result
df['audio_language_id'] = audio_language_groupid_result
df['supported_language_id'] = supported_language_groupid_result
df['language_groupid'] = language_groupid_result
df['support_groupid'] = support_groupid_result
df['tags_groupid'] = tags_groupid_result

#setup the tables
#setup packages nested json
dfpackages = (df['packages'].copy())
dfpackages = dfpackages.rename_axis('game_id').reset_index()
dfpackages = dfpackages.explode('packages')
dfnormalizedpackages = pd.json_normalize(dfpackages['packages'])
dfpackages = pd.concat([dfpackages.reset_index(drop=True),dfnormalizedpackages.reset_index(drop=True)], axis=1)
dfpackages = dfpackages.drop(columns='packages')
dfpackages = dfpackages.rename_axis('package_id').reset_index()
dfpackages['package_id'] = dfpackages['package_id'].apply(lambda x: x+1)

#setup sub-packages nested json
dfsubpackages = dfpackages.copy()
dfsubpackages = dfsubpackages.drop(columns=['game_id','title','description'])
dfsubpackages = dfsubpackages.explode('subs')
dfsubpackages = dfsubpackages.dropna()
dfnormalizedsubpackages = pd.json_normalize(dfsubpackages['subs'])
dfsubpackages = pd.concat([dfsubpackages.reset_index(drop=True),dfnormalizedsubpackages.reset_index(drop=True)], axis=1)
dfsubpackages = dfsubpackages.drop(columns='subs')
dfsubpackages = dfsubpackages.rename_axis('subpackage_id').reset_index()
dfsubpackages = dfsubpackages.dropna()
dfsubpackages = dfsubpackages.replace({np.nan: "none"})

#drop subs from package as subs is not needed anymore
dfpackages = dfpackages.drop(columns='subs')
dfpackages = dfpackages.replace({np.nan: "none"})

#screenshots
dfscreenshots = df['screenshots'].copy()
dfscreenshots = dfscreenshots.rename_axis('game_id').reset_index()
dfscreenshots = dfscreenshots.explode('screenshots')
dfscreenshots = dfscreenshots.dropna()
dfscreenshots = dfscreenshots.drop_duplicates()

#movies 
dfmovies = df['movies'].copy()
dfmovies = dfmovies.rename_axis('game_id').reset_index()
dfmovies = dfmovies.explode('movies')
dfmovies = dfmovies.dropna()

#developers
dfdevelopers = df[['developer_groupid','developers']].copy()
dfdevelopers = dfdevelopers.explode('developers')
dfdevelopers = dfdevelopers.replace({np.nan: "none"})
dfdevelopers = dfdevelopers.drop_duplicates()

#publishers
dfpublishers = df[['publisher_groupid','publishers']].copy()
dfpublishers = dfpublishers.explode('publishers')
dfpublishers = dfpublishers.replace({np.nan: "none"})
dfpublishers = dfpublishers.drop_duplicates()

#category
dfcategory = df[['category_groupid','categories']].copy()
dfcategory = dfcategory.explode('categories')
dfcategory = dfcategory.replace({np.nan: "none"})
dfcategory = dfcategory.drop_duplicates()

#genre
dfgenre = df[['genre_groupid','genres']].copy()
dfgenre = dfgenre.explode('genres')
dfgenre = dfgenre.replace({np.nan: "none"})
dfgenre = dfgenre.drop_duplicates()

#audio language
dfaudio = df[['audio_language_id','full_audio_languages']].copy()
dfaudio = dfaudio.explode('full_audio_languages')
dfaudio = dfaudio.replace({np.nan: "none"})
dfaudio = dfaudio.drop_duplicates()

#text language
dftext = df[['supported_language_id','supported_languages']].copy()
dftext = dftext.explode('supported_languages')
dftext = dftext.replace({np.nan: "none"})
dftext = dftext.drop_duplicates()

#full language
dflang = df[['language_groupid','audio_language_id','supported_language_id']].copy()
dflang = dflang.drop_duplicates()
dflang = dflang.drop(columns=['audio_language_id','supported_language_id'])

#tags language
dftags = df[['tags_groupid','tags']].copy()
dftags['tags'] = dftags['tags'].apply(lambda x: tuple(x.items()) if x else x)
dftags = dftags.explode('tags')
dftags = dftags.replace({np.nan: "none"})
dftags['name'] = dftags['tags'].apply(lambda x: x[0] if isinstance(x, tuple) else x)
dftags['count'] = dftags['tags'].apply(lambda x: x[1] if isinstance(x, tuple) else x)
dftags['count'] = dftags['count'].apply(lambda x: 0 if x == 'none' else x)
dftags = dftags.drop(columns='tags')
dftags = dftags.drop_duplicates()

#supported OS
dfsup = df[['support_groupid','windows','mac','linux']].copy()
dfsup = dfsup.drop_duplicates()

#date
date = pd.DataFrame(pd.to_datetime(df['release_date'].copy(),format='mixed'))
date['year'] = date['release_date'].dt.year
date['month'] = date['release_date'].dt.month
date['quarter'] = date['release_date'].dt.quarter
date = date.drop_duplicates()

#ga
df['release_date'] = pd.to_datetime(df['release_date'],format='mixed')
df = df.rename_axis('id').reset_index()

#drop the normalized columns
df = df.drop(columns=["genres","supported_languages","full_audio_languages","packages",
"developers","publishers","categories","screenshots","movies","tags","windows","mac","linux",'audio_language_id',
 'supported_language_id'])

 #rearrange columns
df = df[['id','name', 'about_the_game','detailed_description','short_description',
 'reviews','header_image','website','support_url','support_email','price','release_date',
 'required_age','dlc_count','achievements','average_playtime_forever','average_playtime_2weeks',
 'median_playtime_forever','median_playtime_2weeks','peak_ccu','metacritic_score','metacritic_url',
 'notes', 'score_rank','user_score','positive','negative','estimated_owners',
 'recommendations','genre_groupid','language_groupid','developer_groupid',
 'publisher_groupid','category_groupid','support_groupid', 'tags_groupid']]

 #rename columns
df.columns = ['id','name', 'about','detailedDesc','shortDesc',
 'reviews','headerImg','website','supportURL','supportEmail','price','releaseDate',
 'requiredAge','dlcCount','achievements','avePlaytimeForever','avePlaytime2Weeks',
 'medPlaytimeForever','medPlaytime2Weeks','peakCCU','metacriticScore','metacriticURL',
 'notes', 'scoreRank','userScore','positiveReviews','negativeReviews','estimatedOwners',
 'recommendations','dimGenreId','dimLanguageId','dimDeveloperId',
 'dimPublisherId','dimCategoryId','dimSupportId', 'dimTagId']

 #renaming columns to match the database
dftext.columns = ['groupId','language']
dfaudio.columns = ['groupId','language']
dflang.columns = ['dimLanguageId']
dfpackages.columns = ['id','gameId','title','description']
dfsubpackages.columns = ['id','dimPackageId','text','description','price']
dfdevelopers.columns = ['groupId','name']
dfdevelopersdim = pd.DataFrame(dfdevelopers['groupId'].copy().drop_duplicates())
dfdevelopersdim.columns = ['dimDeveloperId']
dfpublishers.columns = ['groupId','name']
dfpublishersdim = pd.DataFrame(dfpublishers['groupId'].copy().drop_duplicates())
dfpublishersdim.columns = ['dimPublisherId']
dfcategory.columns = ['groupId','name']
dfcategorydim = pd.DataFrame(dfcategory['groupId'].copy().drop_duplicates())
dfcategorydim.columns = ['dimCategoryId']
dfgenre.columns = ['groupId','genre']
dfgenredim = pd.DataFrame(dfgenre['groupId'].copy().drop_duplicates())
dfgenredim.columns = ['dimGenreId']
dfscreenshots.columns = ['gameId','url']
dfmovies.columns = ['gameId','url']
dftags.columns = ['groupId','name','count']
dftagsdim = pd.DataFrame(dftags['groupId'].copy().drop_duplicates())
dftagsdim.columns = ['dimTagId']
dfsup.columns = ['supportId','windowsSupport','macSupport','linuxSupport']
date.columns = ['date','year','month','quarter']

#removing special cases
dfpublishers = dfpublishers[dfpublishers['name'] != 'Studio Élan']

#setup sql connection
load_dotenv(override=True)
engine = create_engine(os.environ.get("ALCHEMY_DATABASE_URL"), connect_args={'connect_timeout': 600})

#loading the data into the database
date.to_sql(name='dimdate', con=engine, if_exists='append', index=False,method="multi")
dflang.to_sql(name='dimlanguage', con=engine, if_exists='append', index=False)
dfaudio.to_sql(name='audiolanguage', con=engine, if_exists='append', index=False)
dftext.to_sql(name='textlanguage', con=engine, if_exists='append', index=False)
dfdevelopersdim.to_sql(name='dimdeveloper', con=engine, if_exists='append', index=False)
dfdevelopers.to_sql(name='developer', con=engine, if_exists='append', index=False)
dfpublishersdim.to_sql(name='dimpublisher', con=engine, if_exists='append', index=False)
dfpublishers.to_sql(name='publisher', con=engine, if_exists='append', index=False)
dfcategorydim.to_sql(name='dimcategory', con=engine, if_exists='append', index=False)
dfcategory.to_sql(name='category', con=engine, if_exists='append', index=False)
dfgenredim.to_sql(name='dimgenre', con=engine, if_exists='append', index=False)
dfgenre.to_sql(name='genre', con=engine, if_exists='append', index=False)
dftagsdim.to_sql(name='dimtag', con=engine, if_exists='append', index=False)
dftags.to_sql(name='tag', con=engine, if_exists='append', index=False)
dfsup.to_sql(name='dimsupport', con=engine, if_exists='append', index=False)
df.to_sql(name='game_fact_table', con=engine, if_exists='append', index=False, chunksize=2000)
dfmovies.to_sql(name='dimmovie', con=engine, if_exists='append', index=False, chunksize=2000)
dfscreenshots.to_sql(name='dimscreenshot', con=engine, if_exists='append', index=False, chunksize=2000)
dfpackages.to_sql(name='dimpackage', con=engine, if_exists='append', index=False)
dfsubpackages.to_sql(name='dimpackagesub', con=engine, if_exists='append', index=False)


