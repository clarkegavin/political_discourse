#preprocessing/conversation_segment_splitter.py
from preprocessing.base import Preprocessor
import pandas as pd
from logs.logger import get_logger



class ConversationSegmentSplitter(Preprocessor):
    def __init__(self, max_comments: int, max_words: int, max_days_inactive: int):
        self.max_comments = max_comments
        self.max_words = max_words
        self.max_days_inactive = max_days_inactive
        self.split_due_to_max_comments = 0
        self.split_due_to_max_words = 0
        self.split_due_to_max_days_inactive = 0
        self.logger = get_logger(self.__class__.__name__)

    def fit(self, data: pd.DataFrame, **kwargs):
        """
        Fit method for compatibility. No fitting required for this preprocessor.
        """
        return self

    def transform(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        self.logger.info("Starting ConversationSegmentSplitter with max_comments=%d, max_words=%d, max_days_inactive=%d",
                    self.max_comments, self.max_words, self.max_days_inactive)

        self.logger.info(f"ConversationSegmentSplitter available columns: {data.columns.tolist()}")
        split_documents = []

        for _, row in data.iterrows():
            comments = row['CommentRecords']
            word_count = 0
            comment_count = 0
            segment_start_idx = 0
            segment_start_date = row['DocumentStartDate']
            segments = []
            split_reason = None

            for idx, comment in enumerate(comments):
                comment_count += 1
                word_count += comment['CommentWordCount']

                if idx > 0:

                    prev_date = comments[idx - 1]['CommentDateInserted']
                    current_date = comment['CommentDateInserted']
                    days_inactive = (current_date - prev_date).days


                    if comment_count > self.max_comments:
                        self.split_due_to_max_comments += 1
                        split_reason = "max_comments"
                    elif word_count > self.max_words:
                        self.split_due_to_max_words += 1
                        split_reason = "max_words"
                    elif days_inactive > self.max_days_inactive:
                        self.split_due_to_max_days_inactive += 1
                        split_reason = "max_days_inactive"

                    # if (comment_count > self.max_comments or
                    #     word_count > self.max_words or
                    #     days_inactive > self.max_days_inactive):
                    #
                    if split_reason:
                        self.logger.info("Splitting document %s due to %s at comment index %d", row['DocumentID'], split_reason, idx)
                        segments.append({
                            'DocumentID': f"{row['DocumentID']}_part{len(segments) + 1}",
                            'DiscussionID': row['DiscussionID'],
                            'DiscussionTitle': row['DiscussionTitle'],

                            'DiscussionBody': row['DiscussionBody'],
                            'DiscussionCategoryID': row['DiscussionCategoryID'],
                            'DiscussionDateInserted': row['DiscussionDateInserted'],
                            'DocumentType': row['DocumentType'],
                            'DocumentStartDate': segment_start_date,
                            'DocumentEndDate': prev_date,
                            'ChainID': row['ChainID'],
                            'CommentRecords': comments[segment_start_idx:idx],
                            'CommentCount': comment_count - 1,
                            'CommentWordCount': word_count - comment['CommentWordCount'],
                            'CommentCharacterCount': sum(c['CommentCharacterCount'] for c in comments[segment_start_idx:idx]),
                            'DurationHours': (prev_date - segment_start_date).total_seconds() / 3600,
                            'SplitReason': split_reason
                        })

                        segment_start_idx = idx
                        segment_start_date = current_date
                        comment_count = 1
                        word_count = comment['CommentWordCount']

            # Add the last segment
            segments.append({
                'DocumentID': f"{row['DocumentID']}_part{len(segments) + 1}",
                'DiscussionID': row['DiscussionID'],
                'DiscussionTitle': row['DiscussionTitle'],
                'DiscussionBody': row['DiscussionBody'],
                'DiscussionCategoryID': row['DiscussionCategoryID'],
                'DiscussionDateInserted': row['DiscussionDateInserted'],
                'DocumentType': row['DocumentType'],
                'DocumentStartDate': segment_start_date,
                'DocumentEndDate': row['DocumentEndDate'],
                'ChainID': row['ChainID'],
                'CommentRecords': comments[segment_start_idx:],
                'CommentCount': len(comments) - segment_start_idx,
                'CommentWordCount': word_count,
                'CommentCharacterCount': sum(c['CommentCharacterCount'] for c in comments[segment_start_idx:]),
                'DurationHours': (row['DocumentEndDate'] - segment_start_date).total_seconds() / 3600,
                'SplitReason': split_reason
            })

            split_documents.extend(segments)

        self.logger.info("Finished splitting documents. Total segments created: %d", len(split_documents))
        self.logger.info("Splits due to max_comments: %d, max_words: %d, max_days_inactive: %d",
                         self.split_due_to_max_comments, self.split_due_to_max_words,
                         self.split_due_to_max_days_inactive)

        return pd.DataFrame(split_documents)
