from pipelines.steam_pipeline import SteamDataPipeline
from logs.logger import get_logger
from dotenv import load_dotenv
import os


if __name__ == "__main__":

    load_dotenv()
    logger = get_logger("steam_api_extract")
    api_key = os.getenv("STEAM_API_KEY")
    if not api_key:
        logger.error("STEAM_API_KEY not found in environment variables.")
        exit(1)

    logger.info("Starting Steam data extraction pipeline...")

    steam_pipeline = SteamDataPipeline(
        api_key=api_key,
        chunk_size=100,
        limit_apps=50000,
        offset_apps=48832  #
    )
    logger.info("Running Steam data extraction pipeline...")
    steam_pipeline.run()
    logger.info("Steam data extraction pipeline completed.")
