# pipelines/oireachtas_pipeline.py
from fetchers.factory import FetcherFactory
from fetchers.context import FetcherContext
from data.sqlalchemy_connector import SQLAlchemyConnector
from data.extractors import (OireachtasQuestionExtractor, OireachtasDebateExtractor)
from data.models import OireachtasQuestion
from logs.logger import get_logger
import pandas as pd


class OireachtasDataPipeline:

    def __init__(self, connector=None,  api_key=None, chunk_size=100,
        date_start = None,  date_end = None ):
        self.logger = get_logger(self.__class__.__name__)
        self.connector = connector or SQLAlchemyConnector()
        self.connector.create_tables(base=OireachtasQuestion.__base__)

        self.context = FetcherContext(api_key=api_key)
        self.date_start = date_start
        self.date_end = date_end

        self.question_fetcher = FetcherFactory.create(
            "oireachtas_questions",
            context=self.context,
        )


        self.debate_fetcher = FetcherFactory.create(
            "oireachtas_debates",
            context=self.context,
        )

        self.question_extractor = OireachtasQuestionExtractor(
            connector=self.connector,
            chunk_size=chunk_size,
        )

        self.debate_extractor = OireachtasDebateExtractor(
            connector=self.connector,
            chunk_size=chunk_size,
        )

    @classmethod
    def from_config(cls, cfg: dict):
        params = cfg.get("params", {})
        return cls(**params)

    def execute(self, data=None):

        if not self.date_start or not self.date_end:
            raise ValueError("date_start and date_end must be set before executing the pipeline")

        # 1 Questions
        self.logger.info("Fetching questions")
        try:
            questions = self.question_fetcher.fetch(
                date_start=self.date_start,
                date_end=self.date_end,
            )
        except Exception as e:
            self.logger.error(e)
        self.question_extractor.save_data(questions)

        self.logger.info("Oireachtas pipeline completed")

        return questions


