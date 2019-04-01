# -*- coding: utf-8 -*-
"""Lucene index module.
"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

from java.io import File
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document
from org.apache.lucene.document import Field, StringField, TextField, \
    StoredField
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.util import Version
import logging

logger = logging.getLogger(__name__)


class Indexer():
    """This class provide functions to index article stored in the database."""

    def __init__(self, index_dir, mode, date_format='%Y-%m-%dT%H:%M:%S'):
        """Constructor of Indexer.

        Parameters
        ----------
        index_dir : string
            The location of lucene index
        mode : string
            The mode when opening lucene index. Available values are:
                'create', open new index and overwriting over index,
                'append', open existed index and append.
                'create_or_append', if `index_dir` exists, 'append',
                else 'create'
        date_format : string
            We save datetime field as string, `date_format` specify how to
            format datetime into string.
        """
        # self.store = FSDirectory.open(File(index_dir))
        self.store = FSDirectory.open(Paths.get(index_dir))
        # self.analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)
        self.analyzer = StandardAnalyzer()
        # self.config = IndexWriterConfig(Version.LUCENE_CURRENT, self.analyzer)
        self.config = IndexWriterConfig(self.analyzer)
        self.mode = mode
        self.date_format = date_format
        if mode == 'create_or_append':
            self.config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
        elif mode == 'create':
            self.config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        elif mode == 'append':
            self.config.setOpenMode(IndexWriterConfig.OpenMode.APPEND)
        else:
            raise ValueError('Invalid mode %s', mode)
        self.writer = IndexWriter(self.store, self.config)

    def index_one(self, article):
        """Create index for one url object in the database.
        """
        try:
            date_published_str = article['date_published'].strftime(
                self.date_format)
        except Exception as e:
            logger.warning('Error when formating date_published %r: %s ',
                           article['canonical_url'], e)
            return
        doc = Document()
        doc.add(StoredField('group_id', article['group_id']))
        doc.add(StoredField('article_id', article['article_id']))
        doc.add(
            StringField('date_published', date_published_str, Field.Store.YES))
        doc.add(StringField('domain', article['domain'], Field.Store.YES))
        doc.add(StringField('site_type', article['site_type'], Field.Store.YES))
        doc.add(
            TextField('canonical_url', article['canonical_url'],
                      Field.Store.YES))
        doc.add(TextField('title', article['title'], Field.Store.YES))
        doc.add(TextField('meta', article['meta'], Field.Store.NO))
        doc.add(TextField('content', article['content'], Field.Store.NO))
        doc.add(StoredField('uq_id_str', article['uq_id_str']))
        self.writer.addDocument(doc)

    def close(self):
        """Close the index writer."""
        self.writer.close()
