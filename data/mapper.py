from .models import SteamGame
from datetime import datetime
from typing import Optional
from dateutil import parser
from datetime import date, datetime


def map_payload_to_steamgame(payload: dict) -> dict:
    # Flatten simple fields

    release_str = payload.get("release_date", {}).get("date") if payload.get("release_date") else None

    mapped = {
        "AppID": payload.get("steam_appid"),
        "Name": payload.get("name"),
        "Type": payload.get("type"),
        "Is_Free": payload.get("is_free"),
        # "Short_Description": payload.get("short_description"),
        # "Detailed_Description": payload.get("detailed_description"),
        # "About_The_Game": payload.get("about_the_game"),
        "Supported_Languages": payload.get("supported_languages"),
        #"Website": payload.get("website"),
        "Developers": ",".join(payload.get("developers", [])) if payload.get("developers") else None,
        "Publishers": ",".join(payload.get("publishers", [])) if payload.get("publishers") else None,
        "Platforms": ",".join([k for k, v in payload.get("platforms", {}).items() if v]) if payload.get(
            "platforms") else None,
        "Metacritic_Score": payload.get("metacritic", {}).get("score") if payload.get("metacritic") else None,
        "Metacritic_URL": payload.get("metacritic", {}).get("url") if payload.get("metacritic") else None,
        "Categories": ",".join([c.get("description") for c in payload.get("categories", [])]) if payload.get(
            "categories") else None,
        "Genres": ",".join([g.get("description") for g in payload.get("genres", [])]) if payload.get(
            "genres") else None,
        "Tags": ",".join([t.get("description") for t in payload.get("tags", [])]) if payload.get("tags") else None,
        "Price_Currency": payload.get("price_overview", {}).get("currency") if payload.get("price_overview") else None,
        "Price_Initial": payload.get("price_overview", {}).get("initial") if payload.get("price_overview") else None,
        "Price_Final": payload.get("price_overview", {}).get("final") if payload.get("price_overview") else None,
        "Price_Discount_Percent": payload.get("price_overview", {}).get("discount_percent") if payload.get(
            "price_overview") else None,
        # Summary review metrics
        "Total_Reviews": payload.get("total_reviews"),
        "Total_Positive": payload.get("total_positive"),
        "Total_Negative": payload.get("total_negative"),
        "Review_Score": payload.get("review_score"),
        "Review_Score_Desc": payload.get("review_score_desc"),
        "Recommendations": payload.get("recommendations", {}).get("total") if payload.get("recommendations") else None,
        # "Release_Date": payload.get("release_date", {}).get("date") if payload.get("release_date") else None,
        "Release_Date": parse_steam_release_date(release_str),
        "Content_Descriptors": ",".join(str(i) for i in payload.get("content_descriptors", {}).get("ids", [])) if payload.get("content_descriptors") else None,
        "Current_Players": payload.get("current_players"),
        "Current_Players_Last_Updated":payload.get("current_players_fetched_at"),
        "URL": f"https://store.steampowered.com/app/{payload.get('steam_appid')}"
    }

    return mapped

    # Reviews info
    reviews = payload.get("reviews", [])
    if reviews:
        total_reviews = len(reviews)
        positive_reviews = sum(1 for r in reviews if r.get("review_score", 0) > 0)
        negative_reviews = sum(1 for r in reviews if r.get("review_score", 0) <= 0)

        mapped.update({
            "Total_Reviews": total_reviews,
            "Positive_Reviews": positive_reviews,
            "Negative_Reviews": negative_reviews
        })
    else:
        mapped.update({
            "Total_Reviews": None,
            "Positive_Reviews": None,
            "Negative_Reviews": None
        })

    return mapped

def parse_steam_release_date(release_str: Optional[str]) -> Optional[date]:
    """Parse Steam release date string to a datetime.date object."""
    if not release_str:
        return None

    from datetime import datetime
    import re

    release_str = release_str.strip()

    # Sometimes Steam uses "1 Jan, 2020" or just "Jan, 2020" or "Coming Soon"
    if release_str.lower() == "coming soon":
        return None

    # Try day + month + year
    try:
        dt = datetime.strptime(release_str, "%d %b, %Y")
        return dt  # datetime object
    except ValueError:
        pass

    # Try month + year
    try:
        dt = datetime.strptime(release_str, "%b, %Y")
        return dt
    except ValueError:
        pass

    # fallback: return None if can't parse
    return None
