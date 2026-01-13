from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service
import time
import pandas as pd
import requests

# =========================
# CONFIG
# =========================
EDGE_DRIVER_PATH = r"C:\Users\Gavin\Tools\msedgedriver.exe"  # update path if needed
MAX_SCROLLS = 30  # number of times to scroll to load more games
CSV_FILE = "roblox_games_sql.csv"

# APIs
UNIVERSE_API = "https://apis.roblox.com/universes/v1/places/{placeId}/universe"
GAMES_API = "https://games.roblox.com/v1/games"

# =========================
# STEP 1: Scrape place IDs from charts
# =========================
def get_place_ids():
    print("Launching Edge browser to scrape charts...")
    options = webdriver.EdgeOptions()
    options.add_argument("--headless")  # run in background
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    service = Service(executable_path=EDGE_DRIVER_PATH)
    driver = webdriver.Edge(service=service, options=options)
    driver.get("https://www.roblox.com/charts?device=computer&country=all")
    time.sleep(3)

    # Scroll to load more games
    for _ in range(MAX_SCROLLS):
        print(f"Scrolling to load more games...")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

    # Extract place IDs
    elements = driver.find_elements(By.CSS_SELECTOR, "a[href*='/games/']")
    place_ids = set()
    for el in elements:
        href = el.get_attribute("href")
        parts = href.split("/")
        try:
            pid = int(parts[4])
            place_ids.add(pid)
        except:
            continue

    driver.quit()
    print(f"Collected {len(place_ids)} place IDs")
    return list(place_ids)

# =========================
# STEP 2: Convert place IDs → universe IDs
# =========================
def place_to_universe(place_id):
    try:
        resp = requests.get(UNIVERSE_API.format(placeId=place_id))
        resp.raise_for_status()
        return resp.json().get("universeId")
    except:
        return None

def get_universe_ids(place_ids):
    universe_ids = []
    for pid in place_ids:
        uid = place_to_universe(pid)
        if uid:
            universe_ids.append(uid)
        time.sleep(0.1)
    print(f"Collected {len(universe_ids)} universe IDs")
    return universe_ids

# =========================
# STEP 3: Get game metadata
# =========================
def get_game_details(universe_ids):
    rows = []
    BATCH_SIZE = 50
    for i in range(0, len(universe_ids), BATCH_SIZE):
        batch = universe_ids[i:i+BATCH_SIZE]
        ids_str = ",".join(str(x) for x in batch)
        try:
            resp = requests.get(f"{GAMES_API}?universeIds={ids_str}")
            resp.raise_for_status()
            data = resp.json().get("data", [])
            for g in data:
                rows.append({
                    "gameID": g.get("id"),
                    "Title": g.get("name"),
                    "Description": g.get("description"),
                    "Genre": g.get("genre"),
                    "Creator": g.get("creator", {}).get("name"),
                    "URL": f"https://www.roblox.com/games/{g.get('rootPlaceId')}",
                    "Active_Users": g.get("playing"),
                    "Favorites": g.get("favoriteCount"),
                    "Total_Visits": g.get("visits"),
                    "Date_Created": g.get("created"),
                    "Last_Updated": g.get("updated"),
                    "Server_Size": g.get("maxPlayers"),
                })
        except:
            continue
        time.sleep(0.2)
    print(f"Collected {len(rows)} games")
    return rows

# =========================
# STEP 4: Export CSV
# =========================
def export_to_csv(rows):
    df = pd.DataFrame(rows)
    df.to_csv(CSV_FILE, index=False, encoding="utf-8-sig")
    print(f"Saved {len(rows)} games to {CSV_FILE}")

# =========================
# MAIN
# =========================
def main():
    place_ids = get_place_ids()
    universe_ids = get_universe_ids(place_ids)
    rows = get_game_details(universe_ids)
    export_to_csv(rows)


def get_place_ids_by_genre(keyword, max_scrolls=5):
    print(f"Searching for games with keyword: {keyword}")

    options = webdriver.EdgeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    service = Service(executable_path=EDGE_DRIVER_PATH)
    driver = webdriver.Edge(service=service, options=options)

    search_url = f"https://www.roblox.com/discover/?Keyword={keyword}"
    driver.get(search_url)
    time.sleep(3)

    # Scroll to load more games
    for _ in range(max_scrolls):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

    # Extract place IDs
    elements = driver.find_elements(By.CSS_SELECTOR, "a[href*='/games/']")
    place_ids = set()
    for el in elements:
        href = el.get_attribute("href")
        parts = href.split("/")
        try:
            pid = int(parts[4])
            place_ids.add(pid)
        except:
            continue

    driver.quit()
    print(f"Collected {len(place_ids)} place IDs for '{keyword}'")
    return list(place_ids)


if __name__ == "__main__":
    all_rows = []
    # GENRES = ["rpg", "comedy", "adventure", "horror", "sci-fi", "fighting", "military", "naval", "town", "city",  "Build",  "fps", "art", "hero", "shoot em up", "stealth", "survival", "tycoon", "war", "zombie", "rythm"
    #                     , "puzzle", "obby", "parkour", "simulation", "sports", "action", "anime", "battle royale", "cars", "driving", "farming", "fishing", "medieval", "ninja", "pirate", "racing", "roleplay", "space", "training"
    #                     , "surfer", "tower defense", "trading", "wild west", "text"
    #             ]
    # GENRES = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    GENRES = [
        "abandon", "ability", "able", "about", "above", "abroad", "absence", "absolute",
        "absorb", "abstract", "abuse", "academic", "accept", "access", "accident", "accompany",
        "account", "accurate", "accuse", "achieve", "acid", "acknowledge", "acquire", "across",
        "act", "action", "active", "activity", "actor", "actress", "actual", "adapt",
        "add", "addition", "address", "adequate", "adjust", "admire", "adopt", "adult",
        "advance", "advantage", "adventure", "adverse", "advertise", "advice", "advocate", "affect",
        "afford", "afraid", "afternoon", "agency", "agenda", "agent", "aggressive", "ago",
        "agree", "agreement", "agriculture", "ahead", "aid", "aim", "air", "aircraft",
        "airport", "alarm", "album", "alcohol", "alive", "all", "allocate", "allow",
        "almost", "alone", "along", "already", "alter", "alternative", "although", "always",
        "amazing", "ambition", "amend", "amount", "analyse", "analysis", "ancient", "and",
        "angel", "anger", "angle", "animal", "announce", "annual", "another", "answer",
        "anticipate", "anxiety", "any", "anybody", "anyone", "anything", "anyway", "anywhere",
        "apart", "apartment", "apologize", "appeal", "appear", "appearance", "appetite", "applaud",
        "apply", "appoint", "appointment", "appreciate", "approach", "appropriate", "approve", "arch",
        "area", "argue", "argument", "arise", "arm", "army", "around", "arrange",
        "arrest", "arrival", "arrive", "article", "artist", "artistic", "as", "ashamed",
        "aside", "ask", "aspect", "assemble", "assess", "asset", "assign", "assist",
        "assistance", "assistant", "associate", "association", "assume", "assumption", "assure", "astonish",
        "at", "athlete", "atmosphere", "attach", "attack", "attempt", "attend", "attention",
        "attitude", "attorney", "attract", "attribute", "audience", "author", "authority", "auto",
        "available", "average", "avoid", "await", "awake", "award", "aware", "away",
        "awful", "awkward", "baby", "back", "background", "bacteria", "bad", "badly",
        "bag", "balance", "ball", "ban", "band", "bank", "bar", "barely",
        "bargain", "barrier", "base", "baseball", "basic", "basically", "basis", "basket",
        "basketball", "bath", "battery", "battle", "beach", "bear", "beard", "beat",
        "beautiful", "beauty", "because", "become", "bed", "bedroom", "beer", "before",
        "begin", "beginning", "behave", "behavior", "behind", "being", "belief", "believe",
        "bell", "belong", "below", "bend", "benefit", "beside", "besides", "best",
        "betray", "better", "between", "beyond", "bicycle", "bid", "big", "bike",
        "bill", "billion", "bind", "biology", "bird", "birth", "birthday", "biscuit",
        "bite", "bitter", "black", "blade", "blame", "blank", "blanket", "blind",
        "block", "blog", "blood", "blow", "blue", "board", "boast", "boat",
        "body", "boil", "bomb", "bond", "bone", "bonus", "book", "boom",
        "boot", "border", "borrow", "boss", "both", "bother", "bottle", "bottom",
        "boundary", "bowl", "box", "boy", "brain", "branch", "brand", "brave",
        "bread", "break", "breakfast", "breast", "breath", "breathe", "breed", "brick",
        "bridge", "brief", "bright", "brilliant", "bring", "British", "broad", "broadcast",
        "broken", "brother", "brow", "brown", "brush", "brutal", "bubble", "budget",
        "build", "building", "bullet", "bump", "bunch", "burden", "burn", "burst",
        "bury", "business", "busy", "but", "butter", "button", "buy", "buyer",
        "by", "cabin", "cable", "cafe", "cake", "calculate", "calculation", "call",
        "calm", "camera", "camp", "campaign", "campus", "can", "cancel", "cancer",
        "candidate", "candy", "cap", "capable", "capacity", "capital", "captain", "capture",
        "car", "carbon", "card", "care", "career", "careful", "careless", "carpet",
        "carry", "cart", "case", "cash", "casino", "cast", "castle", "cat",
        "catalog", "catch", "category", "cause", "cautious", "cave", "ceiling", "celebrate",
        "celebration", "celebrity", "cell", "center", "central", "century", "ceremony", "certain",
        "certainly", "chain", "chair", "chairman", "challenge", "chamber", "champion", "chance",
        "change", "channel", "chapter", "character", "characteristic", "charge", "charity", "chart",
        "chase", "chat", "cheap", "cheat", "check", "cheek", "cheese", "chef",
        "chemical", "chemistry", "chest", "chew", "chicken", "chief", "child", "childhood",
        "chill", "chin", "chocolate", "choice", "choose", "chop", "church", "cigarette",
        "circle", "circumstance", "citizen", "city", "civil", "claim", "class", "classic",
        "classical", "classroom", "clean", "clear", "clearly", "clerk", "clever", "client",
        "climate", "climb", "clinic", "clinical", "clock", "close", "closet", "cloth",
        "clothes", "clothing", "cloud", "club", "clue", "coach", "coal", "coast",
        "coat", "code", "coffee", "cognitive", "cold", "collapse", "collar", "colleague",
        "collect", "collection", "college", "colon", "colony", "color", "column", "combination",
        "combine", "come", "comedy", "comfort", "comfortable", "command", "comment", "commercial",
        "commission", "commit", "commitment", "committee", "common", "communicate", "communication", "community",
        "company", "compare", "comparison", "compete", "competition", "competitive", "complain", "complaint",
        "complete", "completely", "complex", "component", "compose", "composition", "comprehensive", "computer",
        "concentrate", "concept", "concern", "concert", "conclude", "conclusion", "concrete", "condition",
        "conduct", "conference", "confess", "confidence", "confident", "confirm", "conflict", "confront",
        "confuse", "confused", "confusion", "congratulations", "connect", "connection", "conscious", "consequence",
        "conservative", "consider", "considerable", "consideration", "consist", "consistent", "constant", "constantly",
        "constitute", "construct", "construction", "consult", "consumer", "contact", "contain", "container",
        "contemporary", "content", "contest", "context", "continue", "continued", "continuous", "contract",
        "contrast", "contribute", "contribution", "control", "controversial", "conversation", "convert", "convince",
        "cook", "cookie", "cooking", "cool", "cooperate", "cooperation", "cooperative", "coordinate",
        "coordination", "copy", "core", "corn", "corner", "corporate", "correct", "correlate",
        "correspond", "cost", "costly", "cottage", "cotton", "couch", "could", "council",
        "count", "counter", "country", "countryside", "couple", "courage", "course", "court",
        "cousin", "cover", "cow", "cowboy", "craft", "crash", "crazy", "cream",
        "create", "creation", "creative", "creature", "credit", "crew", "crime", "criminal",
        "crisis", "criteria", "critic", "critical", "criticism", "criticize", "crop", "cross",
        "crow", "crowd", "crucial", "cruel", "cry", "cultural", "culture", "cup",
        "cupboard", "curious", "currency", "current", "currently", "curriculum", "curve", "customer",
        "custom", "cut", "cute", "cycle"
    ]

    for genre in GENRES:
        print(f"Processing genre: {genre}")
        place_ids = get_place_ids_by_genre(genre)
        universe_ids = get_universe_ids(place_ids)
        rows = get_game_details(universe_ids)

        # Add the search genre to each row
        for r in rows:
            r["Genre_Search"] = genre

        all_rows.extend(rows)
        print(f"Genre '{genre}' processed. Games in Genre: {len(rows)}")
        print(f"Total games collected so far: {len(all_rows)}")
    export_to_csv(all_rows)
