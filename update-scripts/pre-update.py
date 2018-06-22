import logging

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from hoaxy.database import ENGINE as engine
from hoaxy.database.models import A_DEFAULT, A_P_SUCCESS

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    # Delete unnecessary records in table article
    logging.info('Deleting table `article` where `site_id` is null')
    q = "DELETE FROM article WHERE site_id IS NULL"
    engine.execute(text(q))
    logging.info('Table deleted!')

    # DROP NULL CONSTRAINS
    logging.info(
        'DROPPING NULL CONSTRAINS of columns title, meta, and content ...')
    q = """ALTER TABLE article ALTER COLUMN title DROP NOT NULL,
                               ALTER COLUMN meta DROP NOT NULL,
                               ALTER COLUMN content DROP NOT NULL\
        """
    try:
        engine.execute(text(q))
        logging.info('NULL constrains dropped.')
    except SQLAlchemyError as e:
        logging.error(e)

    # Add columns to table article
    q = """
        ALTER TABLE article ADD COLUMN status_code SMALLINT DEFAULT {} ,
                            ADD COLUMN html TEXT\
        """.format(A_DEFAULT)
    logging.info('Adding columns status_code, html to table `article` ...')
    try:
        engine.execute(text(q))
        logging.info('Columns added!')
    except SQLAlchemyError as e:
        logging.error(e)

    logging.info(
        'Updating `status_code` of table `article` for success records ...')
    q = """UPDATE article SET status_code={}
           WHERE site_id IS NOT NULL
                AND title IS NOT NULL
                AND content IS NOT NULL\
        """.format(A_P_SUCCESS)
    try:
        engine.execute(text(q))
        logging.info('Table updated!')
    except SQLAlchemyError as e:
        logging.error(e)
