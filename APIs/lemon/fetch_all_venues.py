from time import sleep
import requests
import json
import os

SAVE_DIR = "C:\\datasets\\lemon_market\\venues_dump.json"


try:
    key = os.environ["LEMON_MARKET_DEFAULT_KEY"] 
    
    response_ = json.loads(requests.get("https://data.lemon.markets/v1/venues?page=1", headers={"Authorization": "Bearer " + key}).content)
    all_venues = response_["results"]
    
    for i in range(2, response_["pages"]):
        response_ = json.loads(requests.get(response_["next"], headers={"Authorization": "Bearer " + key}).content)
        all_venues += response_["results"]
        print("fetch:", i, " / ", response_["pages"])
        sleep(0.61)

    print("save to:", SAVE_DIR)
    json.dump(all_venues, open(SAVE_DIR, "w"))

except Exception as e:
    print("aborded due to: ", e)