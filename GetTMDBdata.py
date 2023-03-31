
import json
import time
from urllib.request import urlopen,quote
import config
import pandas as pd

class  TMDBAPIUtils:

    def __init__(self, api_key:str):
        self.api_key=api_key

    def get_movie(self,query:str,year:int):
        """
        Get basic movie infromation from TMDB api. Takes a query string and returns list of results. 
        Each result is a dictionary of potential movie matches. 
        API docs:https://developers.themoviedb.org/3/search/search-movies
        """
        safe_query=quote(query)
        api_url=f"https://api.themoviedb.org/3/search/movie?api_key={self.api_key}&query={safe_query}&year={year}"
        
        with urlopen(api_url) as response:
            body = response.read()
            api_return=json.loads(body)
        return api_return["results"]
    
    def get_show(self,query:str,first_air_date_year):
        """
        Get basic show infromation from TMDB api. Takes a query string and returns list of results. 
        Each result is a dictionary of potential TV show matches. 
        API Docs: https://developers.themoviedb.org/3/search/search-tv-shows
        """

        safe_query=quote(query)
        api_url=f"https://api.themoviedb.org/3/search/movie?api_key={self.api_key}&query={safe_query}&first_air_date_year={first_air_date_year}"
        
        with urlopen(api_url) as response:
            body = response.read()
            api_return=json.loads(body)
        return api_return["results"]
    
    def get_genres(self,show_name:str):
        """
        Get the genre lookuptable from TMDB. Takes no input and Returns a dictionary of genres 
        https://developers.themoviedb.org/3/genres/get-movie-list
        """

        api_url=f"https://api.themoviedb.org/3/search/tv?api_key={self.api_key}&language=en-US&page=1&query={show_name}&include_adult=false"

        with urlopen(api_url) as response:
            body = response.read()
            api_return=json.loads(body)
        return api_return
    
    def api_call(self,kaggle_df,row_index):
        """Makes the api call to the TMDB api. Takes the active row of the kaggle dataset.
        Returns the original row augmented with the retirieved TMDB data as a pd series.
        Last column reports if match was exact (exactly matching title) or inexact.
        """
        if kaggle_df.loc[row_index,"type"]=="Movie":
            #print("--is movie--")
            api_results=tmdb_api_utils.get_movie(kaggle_df.loc[row_index,"title"],kaggle_df.loc[row_index,"release_year"])
        elif kaggle_df.loc[row_index,"type"]=="TV Show":
            #print("--is show--")
            api_results=tmdb_api_utils.get_show(kaggle_df.loc[row_index,"title"],kaggle_df.loc[row_index,"release_year"])
        else:
            raise Exception(f"Error in determining if title is a tv show or movie {kaggle_df.loc[row_index]}")

        ret_result={}
        for result in api_results:
            #print("checking")
            if result["original_title"]==kaggle_df.loc[row_index,"title"]:
                ret_result={"TMDB_"+key:val for key,val in result.items()}
                ret_result["match_type"]="exact"
                break
        if not ret_result:
            #print("no identical tile, reverting to first result of api query")
            if len(api_results)!=0:
                result=api_results[0]
                ret_result={"TMDB_"+key:val for key,val in result.items()}
            ret_result["match_type"]="inexact"
            match_result=ret_result
        #print("active row")
        #print(kaggle_df.loc[row_index])
        #print("api result")
        return pd.concat([kaggle_df.loc[row_index],pd.Series(ret_result)])
        


    def get_movie_cast(self, movie_id:str, limit:int=None, exclude_ids:list=None) -> list:
        """
        Get the movie cast for a given movie id, with optional parameters to exclude an cast member
        from being returned and/or to limit the number of returned cast members
        documentation url: https://developers.themoviedb.org/3/movies/get-movie-credits

        :param string movie_id: a movie_id
        :param list exclude_ids: a list of ints containing ids (not cast_ids) of cast members  that should be excluded from the returned result
            e.g., if exclude_ids are [353, 455] then exclude these from any result.
        :param integer limit: maximum number of returned cast members by their 'order' attribute
            e.g., limit=5 will attempt to return the 5 cast members having 'order' attribute values between 0-4
            If after excluding, there are fewer cast members than the specified limit, then return the remaining members (excluding the ones whose order values are outside the limit range). 
            If cast members with 'order' attribute in the specified limit range have been excluded, do not include more cast members to reach the limit.
            If after excluding, the limit is not specified, then return all remaining cast members."
            e.g., if limit=5 and the actor whose id corresponds to cast member with order=1 is to be excluded,
            return cast members with order values [0, 2, 3, 4], not [0, 2, 3, 4, 5]
        :rtype: list
            return a list of dicts, one dict per cast member with the following structure:
                [{'id': '97909' # the id of the cast member
                'character': 'John Doe' # the name of the character played
                'credit_id': '52fe4249c3a36847f8012927' # id of the credit, ...}, ... ]
                Note that this is an example of the structure of the list and some of the fields returned by the API.
                The result of the API call will include many more fields for each cast member.
        """
        api_url=f"https://api.themoviedb.org/3/movie/{movie_id}/credits?api_key={self.api_key}&language=en-US"
        retlist=[]
        with urlopen(api_url) as response:
            #print(api_url)
            body = response.read()
            api_return=json.loads(body)
            cast=api_return["cast"]
            if limit is None:
               limit=len(cast)
            if exclude_ids is None:
                exclude_ids=[]
            for cast_member in cast[0:limit]:
                #print(cast_member["order"])
                if (cast_member["id"] not in exclude_ids) and (float(cast_member["order"]<limit)):
                #    print("cast added")
                    retlist.append(cast_member)
        return retlist

    def get_movie_credits_for_person(self, person_id:str, vote_avg_threshold:float=None)->list:
        """
        Using the TMDb API, get the movie credits for a person serving in a cast role
        documentation url: https://developers.themoviedb.org/3/people/get-person-movie-credits

        :param string person_id: the id of a person
        :param vote_avg_threshold: optional parameter to return the movie credit if it is >=
            the specified threshold.
            e.g., if the vote_avg_threshold is 5.0, then only return credits with a vote_avg >= 5.0
        :rtype: list
            return a list of dicts, one dict per movie credit with the following structure:
                [{'id': '97909' # the id of the movie credit
                'title': 'Long, Stock and Two Smoking Barrels' # the title (not original title) of the credit
                'vote_avg': 5.0 # the float value of the vote average value for the credit}, ... ]
        """

        api_url=f"https://api.themoviedb.org/3/person/{person_id}/movie_credits?api_key={self.api_key}&language=en-US"
        with urlopen(api_url) as response:
            body = response.read()
            actord=json.loads(body)
            retlist=[]
            castlist=actord["cast"]
            for movied in castlist:
                if float(movied["vote_average"])>=vote_avg_threshold:
                    retlist.append(movied)
        return retlist
    
if __name__ == "__main__":

    tmdb_api_utils = TMDBAPIUtils(api_key=config.api_key)

    if False: #Create Genre Table?
        #Get Genre Table#
        genre_respose= tmdb_api_utils.get_genres() 
        genre_df=pd.DataFrame.from_dict(genre_respose["genres"])
        #write Genre Table to CSV
        genre_df.to_csv("genres.csv")


    #open kaggle csv of netflix movies
    kaggle_df=pd.read_csv("netflix_titles.csv")
    
    BATCH_SIZE=1000 #define batch size
    STOP_ROW=len(kaggle_df)

    augmented_df=pd.DataFrame() #create a dataframe to hold results
    batch_index=0 
    row_index=0 

    #iterate over kaggle file, writing a copy of the dataframe after adding n files

    print("run is starting")
    while row_index<STOP_ROW:
        
        print(f"starting batch: {batch_index}")
        new_rows=[]
        for _ in range(BATCH_SIZE):
            if row_index%100==0:
                print(f"fetching title number: {row_index}")  
            new_rows.append(tmdb_api_utils.api_call(kaggle_df,row_index))
            row_index+=1
            if row_index>=STOP_ROW:
                break
            time.sleep(0.1)

        augmented_df=pd.concat([augmented_df,pd.DataFrame(new_rows)])
        augmented_df.to_csv(f"netflix_titles_augmented{batch_index}.csv")
        batch_index+=1

    print("End Run")
    #print(mission)

