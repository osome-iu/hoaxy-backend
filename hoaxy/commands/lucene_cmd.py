from hoaxy.commands import HoaxyCommand
from hoaxy.database import Session
from hoaxy.database.functions import get_or_create_m
from hoaxy.database.models import MetaInfo
from hoaxy.utils.log import configure_logging
from hoaxy.ir.index import Indexer
from hoaxy.ir.search import Searcher
from schema import Schema, And, Use, Or
from schema import SchemaError
import lucene
import logging
import sys
import sqlalchemy
import pprint

logger = logging.getLogger(__name__)


class Lucene(HoaxyCommand):
    """
usage:
  hoaxy lucene --index [--mode=<mode>]
  hoaxy lucene --search --query=<q> [--top=<n>]
  hoaxy lucene -h | --help

Using Apache Lucene to build index from the parsed articles. And also
provide a simple interface to query the indexed articles.
--index             Create, append and update index.
--search            Do lucene search

Options:
--mode=<mode>       Mode for create index, available choices are:
                    create_or_append, create, append
                    [default: create_or_append]
--query=<q>         String to query.
--top=<n>           Number of top results to show.
                    [default: 5]
-h --help           Show help.

Examples:

    1. Create index of all non-index documents
        hoaxy lucene --index --mode=create_or_append

    2. If you want to replace the old indexes and create a new one:
        hoaxy lucene --index --mode=create

    3. Search top 5 most relavant article containing keywords 'trump'
        hoaxy lucene --search --query=trump
    """
    name = 'lucene'
    short_description = 'Lucene Indexing and Searching'
    args_schema = Schema({
        '--query': Or(None, lambda s: len(s) > 0),
        '--mode': Or(None, And(Use(str.lower),
                               lambda s: s in ('create_or_append', 'create',
                                               'append'))),
        '--top': Or(None, And(Use(int), lambda x: x > 0)),
        object: object
    })

    @classmethod
    def prepare_article(cls, article_data):
        article_id, group_id, canonical_url, title, meta, content,\
            date_published, domain, site_type = article_data
        article = dict(article_id=article_id,
                       group_id=group_id,
                       canonical_url=canonical_url,
                       title=title,
                       content=content,
                       date_published=date_published,
                       domain=domain,
                       site_type=site_type)
        article['meta'] = unicode(meta)
        article['uq_id_str'] = unicode(group_id) + title
        if article['content'] is None:
            article['content'] = u'NULL'
        return article

    @classmethod
    def index(cls, session, mode, articles_iter, mgid):
        lucene.initVM()
        index_dir = cls.conf['lucene']['index_dir']
        indexer = Indexer(index_dir, mode,
                          date_format=cls.conf['lucene']['date_format'])
        article = None
        for i, data in enumerate(articles_iter):
            article = cls.prepare_article(data)
            indexer.index_one(article)
            if i % cls.conf['window_size'] == 1:
                logger.info('Indexed %s articles', i)
        indexer.close()
        if article is not None:
            mgid.value = str(article['group_id'])
            session.commit()
            logger.info('Indexed article pointer updated!')
        else:
            logger.warning('No new articles are found!')
        logger.info('Done!')

    @classmethod
    def search(cls, query, n):
        lucene.initVM()
        index_dir = cls.conf['lucene']['index_dir']
        searcher = Searcher(index_dir)
        rs = searcher.search(query, n)
        pprint.pprint(rs)

    @classmethod
    def run(cls, args):
        try:
            # print(args)
            args = cls.args_schema.validate(args)
        except SchemaError as e:
            sys.exit(e)
        session = Session()
        # make sure lucene be inited
        lucene.initVM()
        lucene.getVMEnv().attachCurrentThread()
        if args['--index'] is True:
            configure_logging('lucene.index', console_level='INFO')
            mgid = get_or_create_m(session, MetaInfo, data=dict(
                name='article_group_id_lucene_index',
                value='0',
                value_type='int',
                description='article.group_id used for lucene index'),
                fb_uk='name')
            if args['--mode'] == 'create':
                mgid.set_value(0)
                session.commit()
            q = """\
SELECT DISTINCT ON (a.group_id) a.id, a.group_id,
a.canonical_url,
a.title, a.meta, a.content,
coalesce(a.date_published, a.date_captured) AS pd,
s.domain, s.site_type
FROM article AS a
JOIN site AS s ON s.id=a.site_id
WHERE a.site_id IS NOT NULL AND s.is_enabled IS TRUE AND a.group_id>:gid
ORDER BY group_id, pd ASC"""
            articles_iter = session.execute(
                sqlalchemy.text(q).bindparams(gid=mgid.get_value()))
            cls.index(session, args['--mode'], articles_iter, mgid)
        elif args['--search'] is True:
            configure_logging('lucene.search', console_level='INFO')
            cls.search(args['--query'], args['--top'])
        else:
            print("Unrecognized command!")
            sys.exit(2)
