import soccerdata as sd
import dlt 
from dotenv import load_dotenv
import os 
from config import data_library


env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=env_path)


@dlt.resource(name="shot_events", write_disposition="replace")
def get_understat_data(config : dict ,type_of_stat : str , league : str , season : str ):

    """read your config , pass it into here and apply the get attribute method to the understat object to follow a strategy design pattern
    also parametize the leagues and seasons, then add testing
    """
    understat = sd.Understat(leagues=league, seasons=season)

    data_request = getattr(understat , config.get(type_of_stat))

    df = data_request()

    if not df.empty:
        df = df.reset_index()
    else:
        print("Scraper returned an empty dataset!")
        return
    yield df 



pipeline = dlt.pipeline(
    pipeline_name="soccer_data",
    destination="postgres",   
    dataset_name="bronze"     
    )


if __name__ == "__main__":
    #leagues="ENG-Premier League"
    understat = sd.Understat()
    leagues = understat.available_leagues()
    available_seasons = understat.seasons

    #pair sasons and leagues
    leagues_seasons = {league: list(available_seasons) for league in leagues}

    #iterate methods
    for key in data_library:
        #iterate leagues
        for league  , seasons in leagues_seasons.items():
            for season in seasons:
                #print(f"League : { league} , season : {season} , data_request : {data_library.get(key)}")

                soccer_resource = get_understat_data(data_library ,data_library.get(key) , league , season )
                soccer_resource.name = key
                load_info = pipeline.run(soccer_resource)
                print(load_info)
                print("-"*35)

    
      

