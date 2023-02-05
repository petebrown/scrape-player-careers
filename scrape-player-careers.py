from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import concurrent.futures

MAX_THREADS = 30

def construct_url(team_id, season_id):
    url = f'https://www.soccerbase.com/teams/team.sd?team_id={team_id}&teamTabs=stats&season_id={season_id}'
    return url

def get_season_urls():
    team_id = 2598
    season_id = 155

    url = construct_url(team_id, season_id)
    r = requests.get(url)
    doc = BeautifulSoup(r.text, 'html.parser')
    
    season_list = doc.select('#statsSeasonSelectTop option')
    season_ids = [construct_url(team_id, season["value"]) for season in season_list[1:]]

    return season_ids

def get_player_list(url):
    session = requests.Session()
    r = session.get(url)
    doc = BeautifulSoup(r.text, 'html.parser')
    
    player_list = doc.select('table.center tbody tr')

    all_players = []
    for player in player_list:
        player_info = player.select_one('.first')

        player_name = player_info.get_text()
        player_name = player_name.split('(')
        player_name = player_name[0]
        player_name = player_name.strip()
    
        player_url = player_info.select_one('a')['href']
        player_url = f"https://www.soccerbase.com{player_url}"

        player_id = player_url.split("=")[1]

        all_players.append({
            "player_id": player_id,
            "player_name": player_name,
            "player_url": player_url
        })
    return all_players

def get_player_career(player_dict):
    player_id = player_dict["player_id"]
    player_name = player_dict["player_name"]
    url = player_dict["player_url"]

    session = requests.Session()
    r = session.get(url)
    
    career = pd.read_html(r.text)[3]
    career = career[:-2]

    career["player_name"] = player_name
    career["player_id"] = player_id

    career = career[["player_id", "player_name", "CLUB", "FROM", "TO", "FEE"]].rename(columns = {"CLUB": "club",
                              "FROM": "date_joined",
                              "TO": "date_left",
                              "FEE": "fee"})
    career.columns = career.columns.get_level_values(0)

    next_club = pd.DataFrame([{"player_id": player_id,
                               "player_name": player_name,
                               "club": np.nan,
                               "date_joined": np.nan,
                               "date_left": np.nan,
                               "fee": np.nan}])

    first_club = pd.DataFrame([{"player_id": player_id,
                               "player_name": player_name,
                               "club": np.nan,
                               "date_joined": np.nan,
                               "date_left": np.nan,
                               "fee": np.nan}])
    
    career = pd.concat([next_club, career, first_club], ignore_index = False)
    career = career.to_dict(orient="records")

    return career

def async_scraping(scrape_function, urls):
    threads = min(MAX_THREADS, len(urls))
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        results = executor.map(scrape_function, urls)

    return results

def get_transfer_type(player_name, fee):
    if player_name in ['Bailey Passant', 'Cole Stockton', 'Mitch Duggan', 'Ben Jago', 'Ben Maher', 'Danny Harrison', 'Will Vaulks','Mike Jones', 'Richard Hinds', 'Paul Aldridge']:
        return "Trainee"
    elif fee == "Trainee":
        return "Trainee"
    elif fee in ["Free", "Signed", "Undisc."]:
        return "Transfer"
    elif "£" in str(fee):
        return "Transfer"
    elif fee == "Monthly":
        return "Transfer"
    elif fee == "Youth":
        return "Loan"
    else:
        return fee

def date_to_season(date):
    year = date.year
    month = date.month
    day = date.day

    if int(month) <= 5:
        year1 = year - 1
        year2 = str(year)[2:4]
        season = f"{year1}/{year2}"
    elif int(month) > 5:
        year1 = year
        year2 = year + 1
        year2 = str(year2)[2:4]
        season = f"{year1}/{year2}"

    return season

def main():
    season_urls = get_season_urls()    

    player_list = async_scraping(get_player_list, season_urls)
    player_list = list(player_list)
    player_list = [player for sublist in player_list for player in sublist]
    player_list = pd.DataFrame(player_list).drop_duplicates().to_dict(orient="records")

    careers = async_scraping(get_player_career, player_list)
    careers = list(careers)
    careers = [career for sublist in careers for career in sublist]

    df = pd.DataFrame(careers).reset_index(drop=True)
    df["prev_club"] = np.nan
    df["next_club"] = np.nan

    loans = df[((df["club"] == "Tranmere") & (df["fee"] == "Loan")) | ((df["club"] != "Tranmere") & (df["fee"] != "Loan"))].copy()
    loans["prev_club"] = loans.club.shift(-1)
    loans["next_club"] = loans.club.shift(1)

    df.update(loans)

    non_loans = df[df["fee"] != "Loan"].copy()
    non_loans["prev_club"] = non_loans.club.shift(-1)
    non_loans["next_club"] = non_loans.club.shift(1)

    df.update(non_loans)

    multi_loans = df[df.player_id.isin(df[df.prev_club == "Tranmere"].player_id)].copy()
    multi_loans["prev_club"] = multi_loans["prev_club"].shift(-1)
    multi_loans["prev_club"] = multi_loans.apply(lambda x: x.prev_club if x.prev_club == "Tranmere" else x.prev_club, axis=1)
    multi_loans["prev_club1"] = multi_loans["prev_club"].shift(-1)
    multi_loans["prev_club"] = multi_loans.apply(lambda x: x.prev_club1 if x.prev_club == "Tranmere" else x.prev_club, axis=1)
    multi_loans = multi_loans.drop(["prev_club1"], axis=1)
    multi_loans

    df.update(multi_loans)

    df["prev_club"] = df.apply(lambda x: "Trainee" if x.fee == "Trainee" else x.prev_club, axis=1)

    df = df[df["club"] == "Tranmere"].drop_duplicates().reset_index(drop=True)

    df["transfer_type"] = df.apply(lambda x: get_transfer_type(x.player_name, x.fee), axis=1)

    df.loc[df.transfer_type == "Loan", "next_club"] = np.nan

    df["fee"] = df.fee.str.replace("£", "").str.replace(",", "")
    df["date_joined"] = pd.to_datetime(df["date_joined"])
    df["date_left"] = pd.to_datetime(df["date_left"])

    df["season"] = df.apply(lambda x: date_to_season(x.date_joined), axis=1)

    correct_clubs = pd.DataFrame([{"player_id": 73901, "prev_club": "Liverpool"},
                {"player_id": 78589, "prev_club": "Cardiff City"}]).sort_values("player_id")

    club_updates = df[df.player_id.isin(correct_clubs.player_id)].sort_values("player_id").copy()

    club_updates.prev_club = correct_clubs[correct_clubs.player_id.isin(club_updates.player_id)].prev_club.values

    df.update(club_updates)

    df = df.sort_values("player_id", ascending=False, ignore_index=True)

    df["surname"] = df.player_name.str.split(" ").str[-1]
    df = df.sort_values(["surname"]).drop("surname", axis=1)

    return df

df = main()
df.to_csv("./data/player_careers.csv", index=False)