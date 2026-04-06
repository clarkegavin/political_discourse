import sys
import os
# ensure project root (workspace) is on sys.path so top-level packages like `eda` can be imported
sys.path.insert(0, os.getcwd())

import pandas as pd
import traceback

from eda.term_frequency_eda import TermFrequencyEDA


def main():
    try:
        df = pd.DataFrame({
            'DiscussionBody': ['Hello world', None, 'Hello again world'],
            'CommentBody': ['This is a comment', 'Hello world', None],
        })

        eda = TermFrequencyEDA()
        print('Running combine-fields test...')
        res = eda.run(df, fields_to_combine=['DiscussionBody','CommentBody'], combined_field_name='Combined', target_field='Combined')
        print(res.head(10).to_string(index=False))

        print('\nRunning single-target test...')
        res2 = eda.run(df, fields_to_combine=None, target_field='CommentBody')
        print(res2.head(10).to_string(index=False))

        # Test saving to an in-memory sqlite DB
        print('\nRunning save-to-sqlite test...')
        # connector_params accepts db_url for SQLAlchemyConnector
        connector_params = {'db_url': 'sqlite+pysqlite:///:memory:'}
        res3 = eda.run(
            df,
            fields_to_combine=['DiscussionBody','CommentBody'],
            combined_field_name='Combined',
            target_field='Combined',
            table_name='tf_test',
            saver_name='sql_server',
            connector_params=connector_params,
            if_exists='replace',
            chunk_size=500,
        )
        print(res3.head(5).to_string(index=False))

        # Verify table exists by querying via SQLAlchemy
        from data.sqlalchemy_connector import SQLAlchemyConnector
        conn = SQLAlchemyConnector(db_url='sqlite+pysqlite:///:memory:')
        # Note: in-memory DB used above is different connection; persistence across connectors not available.
        # So instead, demonstrate save using a file-backed sqlite DB for verification.
        db_file = 'tests/tf_test.db'
        db_url_file = f'sqlite+pysqlite:///{db_file}'
        if os.path.exists(db_file):
            os.remove(db_file)

        connector_params_file = {'db_url': db_url_file}
        res4 = eda.run(
            df,
            fields_to_combine=['DiscussionBody','CommentBody'],
            combined_field_name='Combined',
            target_field='Combined',
            table_name='tf_test_file',
            saver_name='sql_server',
            connector_params=connector_params_file,
            if_exists='replace',
            chunk_size=500,
        )

        # Now verify table exists in the file-backed DB
        connector_file = SQLAlchemyConnector(db_url=db_url_file)
        engine = connector_file.get_engine()
        from sqlalchemy import inspect as sqinspect
        inspector = sqinspect(engine)
        res_tables = inspector.get_table_names()
        print('\nTables in file-backed sqlite DB:', res_tables)

        print('\nSmoke test completed successfully')
    except Exception as e:
        print('Smoke test failed:')
        traceback.print_exc()


if __name__ == '__main__':
    main()
