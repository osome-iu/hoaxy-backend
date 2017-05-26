# -*- coding: utf-8 -*-
""" Functions for database operations.

    cls : class
        The ORM mapped class.
Note:
    1) SQLAlchemy exceptions are not handled in the following functions (
    except IntegrityError when inserting), so when using these functions,
    you should handle exceptions by yourself.
    2) To avoid concurrency conflict, especially when inserting URLs (we
    have several applications that insert URL into database simultaneously,
    including twitter streaming and scrapy crawling), we commit data into
    database as soon as possible.
    3) We do not delete objects in the database, so please set
    expire_on_commit=False whenusing session. Otherwise, sqlalchemy will
    re-fetch the orm object if you access it again after commit.
"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

from hoaxy.database import ENGINE
from hoaxy.database.models import AssUrlPlatform, Platform
from hoaxy.database.models import Url, Site, AlternateDomain, SiteTag
from sqlalchemy import and_, func, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload, load_only
import logging
import sqlalchemy
import sqlparse

logger = logging.getLogger(__name__)


def get_m(session,
          cls,
          fb_kw=None,
          f_expr=None,
          ob_expr=None,
          limit=None,
          options=None):
    """Query ORMs.

    Parameters
    ----------
    session : object
        An instance of SQLAlchemy Session.
    cls : class
        The ORM model class.
    fb_kw : dict
        The filter_by keywords, used by query.filter_by().
    f_expr : list
        The filter expression, used by query.filter().
    ob_expr : object
        The order by expression, used by query.order_by().
    limit : int
        The limit expression, used by query.limit().
    options : list
        Other query options, e.g, load, join options, used by query.options().

    Returns
    -------
    list
        A list of queried ORM objects.
    """
    q = session.query(Site)
    if fb_kw:
        q = q.filter_by(**fb_kw)
    if f_expr:
        q = q.filter(*f_expr)
    if ob_expr is not None:
        q = q.order_by(ob_expr)
    if options:
        q = q.options(*options)
    if limit:
        q = q.limit(limit)
    return q.all()


def get_max(session, col, fb_kw=None, f_expr=None):
    """Get the maximum value of one specified column.

    Parameters
    ----------
    session : object
        An instance of SQLAlchemy Session.
    col : object
        A column object.
    fb_kw : dict
        The filter_by keywords, used by query.filter_by().
    f_expr : list
        The filter expressions, used by query.filter().

    Returns
    -------
    int
        The maximum value of `col`, if no records match the query, `None` is
        returned.
    """
    q = session.query(func.max(col))
    if fb_kw:
        q = q.filter_by(**fb_kw)
    if f_expr:
        q = q.filter(*f_expr)
    return q.scalar()


def get_msites(session,
               fb_kw=dict(is_enabled=True),
               f_expr=None,
               ob_expr=None,
               limit=None,
               options=[joinedload(Site.alternate_domains),
                        joinedload(Site.site_tags)]):
    """A quick query to fetch sites with frequently used attributes. Returned
    as ORM object.

    Parameters
    ----------
    session : object
        An instance of SQLAlchemy Session.
    fb_kw : dict
        The filter_by keywords, user by query.filter_by().
    f_expr : list
        A list of filter expressions, used by query.filter().
    ob_expr : object
        Order by expression, used by query.order_by().
    limit : int
        Limit expression, used by query.limit().
    options : list
        query options, used by query.options().

    Returns
    -------
    list
        A list of Site objects.
    """
    return get_m(session, Site, fb_kw, f_expr, ob_expr, limit, options)


def get_site_tuples(session,
                    fb_kw=dict(is_enabled=True),
                    f_expr=None,
                    ob_expr=None,
                    limit=None,
                    options=[joinedload(Site.alternate_domains),
                             joinedload(Site.site_tags)]):
    """A quick query to fetch sites with frequently used attributes. Returned
    as tuple.

    Parameters
    ----------
    session : object
        An instance of SQLAlchemy Session.
    fb_kw : dict
        The filter_by keywords, user by query.filter_by().
    f_expr : list
        A list of filter expressions, used by query.filter().
    ob_expr : object
        Order by expression, used by query.order_by().
    limit : int
        Limit expression, used by query.limit().
    options : list
        query options, used by query.options().

    Returns
    -------
    list
        A list of tuple (id, domain).
    """
    r = []
    for ms in get_msites(session, fb_kw=fb_kw, f_expr=f_expr, ob_expr=ob_expr,
                         limit=limit, options=options):
        r.append((ms.id, ms.domain))
        for d in ms.alternate_domains:
            r.append((ms.id, d.name))
    return r


def create_m(session, cls, data):
    """ Insert an orm object into database.

    Parameters
    ----------
    session : object
        An instance of SQLAlchemy Session.
    cls : class
        The ORM mapped class.
    data : dict
        A dict that contains necessary attributes of the ORM objects.

    Notes
    -----
    This is a fast method to insert a record into one table, when
    duplicate happens, the record will be ignored.

    No ORM object is returned!
    """
    session.add(cls(**data))
    try:
        session.commit()
    # Already in db
    except IntegrityError as e:
        logger.debug(e)
        session.rollback()


def create_or_get_m(session, cls, data, fb_uk):
    """Try to insert an record into table, if exist, return it.

    This function first try to insert the record, if fail because of
    duplications. then query it.

    Parameters
    ----------
    session : object
        An instance of SQLAlchemy Session.
    cls : class
        The ORM mapped class.
    data : dict
        A dict that contains necessary attributes of the ORM objects.
    fb_uk : string or list
        The unique columns to identify records in the table.

    Returns
    -------
    object
        The created model object.

    """
    m = cls(**data)
    session.add(m)
    try:
        session.commit()
        return m
    except IntegrityError as e:
        logger.debug(e)
        session.rollback()
        q = session.query(cls)
        # set up filter_by()
        fb = dict()
        if isinstance(fb_uk, basestring):
            fb[fb_uk] = data[fb_uk]
        elif isinstance(fb_uk, (list, tuple)):
            for k in fb_uk:
                fb[k] = data[k]
        q = q.filter_by(**fb)
        return q.one()


def get_or_create_m(session, cls, data,
                    fb_uk=None, fb_kws=None, f_expr=None,
                    onduplicate='ignore', load_cols=None):
    """Try to get one record from table, if not exist, try to insert it.

    This function first try to insert the record, if fail because of
    duplications. then query it.

    Parameters
    ----------
    session : object
        An instance of SQLAlchemy Session.
    cls : class
        The ORM mapped class.
    data : dict
        A dict that contains necessary attributes of the ORM objects.
    fb_uk : string or list
        The unique columns to identify records in the table.
    fb_kw : dict
        The filter_by keywords, user by query.filter_by().
    f_expr : list
        A list of filter expressions, used by query.filter().
    onduplicate : {'ignore', 'update'}
        Handle when duplication happens. If 'ignore', then ignore it. If
        'update', then update the record according the `data`.
    load_cols : list
        A list of columns objects that will be loaded. This is very useful if
        you would like to load only frequently used columns.

    Returns
    -------
    object
        The created or existed model object.

    """
    q = session.query(cls)
    # set up filter_by()
    if fb_uk:
        fb = dict()
        if isinstance(fb_uk, basestring):
            fb[fb_uk] = data[fb_uk]
        elif isinstance(fb_uk, (list, tuple)):
            for k in fb_uk:
                fb[k] = data[k]
        q = q.filter_by(**fb)
    if fb_kws:
        q = q.filter_by(**fb_kws)
    # set up filter()
    if f_expr:
        q = q.filter(*f_expr)
    if load_cols:
        q = q.options(load_only(*load_cols))
    mobj = q.one_or_none()
    if mobj:
        if onduplicate == 'update':
            q.update(data)
            session.commit()
    else:
        mobj = cls(**data)
        session.add(mobj)
        try:
            session.commit()
        except IntegrityError as e:
            logger.warning('Concurrency error %s!', e)
            session.rollback()
            mobj = q.one()
    return mobj


def append_platform_to_url(session, url_id, platform_id):
    """Set platform_id for a URL record.

    The relationship between table url and platform is M:N, so there is a
    association table called ass_url_platform to connect them.

    Parameters
    ----------
    session : object
        An instance of SQLAlchemy Session.
    url_id : int
        The id of a URL record.
    platform_id : int
        The id of a platform record.
    """
    if session.query(AssUrlPlatform.id).filter_by(
            url_id=url_id, platform_id=platform_id).scalar() is None:
        session.add(AssUrlPlatform(url_id=url_id, platform_id=platform_id))
        try:
            session.commit()
        except IntegrityError as e:
            logger.warning('Error Concurrecy conflict %s', e)
            session.rollback()


def get_or_create_murl(session, data,
                       platform_id=None,
                       load_cols=['id', 'date_published']):
    """Get a URL record from table, if not exists, insert it.

    The function is similar as `get_or_create_m`. The difference is how to
    handle duplications. In this function, try to update 'date_published'
    if `data['date_published']` is not None.

    Parameters
    ----------
    session : object
        An instance of SQLAlchemy Session.
    data : dict
        A dict that contains necessary attributes of the ORM objects.
    platform_id : int
        The id of a platform object.
    load_cols : list
        The columns to be loaded. Default is ['id', 'date_published'].

    Returns
    -------
    object
        A URL model object.
    """
    q = session.query(Url).filter_by(raw=data['raw'])\
        .options(load_only(*load_cols))
    murl = q.one_or_none()
    if murl:
        # update date_published if possible
        if murl.date_published is None and \
                data.get('date_published', None) is not None:
            murl.date_published = data['date_published']
            session.commit()
    else:
        murl = Url(**data)
        session.add(murl)
        try:
            session.commit()
        except IntegrityError as e:
            logger.warning('Concurrecy conflict %s', e)
            session.rollback()
            murl = q.one()
    if platform_id is not None:
        append_platform_to_url(session, murl.id, platform_id)
    return murl


def qquery_msite(session, name=None, domain=None):
    """A quick way to query site by its name or domain.

    Parameters
    ----------
    session : object
        An instance of SQLAlchemy Session.
    name : string
        The name of the site to query.
    domain : string
        The primary domain of the site to query.

    Returns
    -------
    object
        The Site ORM object.
    """
    if name is None and domain is None:
        raise TypeError('name or domain are required!')
    q = session.query(Site)
    if name is not None:
        q = q.filter_by(name=name)
    else:
        q = q.filter_by(domain=domain)
    q = q.options(joinedload(Site.alternate_domains),
                  joinedload(Site.site_tags))
    return q.one_or_none()


def get_or_create_msite(session, site,
                        alternate_domains=[],
                        site_tags=[],
                        onduplicate='update'):
    """Get a site from table, if not exist, insert into table.

    This function take cares of the alternate_domains relationi (1:M) and
    site_tags relation (M:N).

    Parameters
    ----------
    session : object
        An instance of SQLAlchemy Session.
    site : dict
        The site data.
    alternate_domains : list
        A list of alternate domain dict with keys 'name' and 'is_alive'.
    site_tags : list
        A list of site tag dict with keys 'name' and 'source'
    onduplicate : {'ignore', 'update'}
        How to handle duplication.
    """
    # FIRST, check whether this site exist
    q = session.query(Site).filter(or_(
        Site.domain.like(site['domain']),
        Site.name.like(site['name']))) .options(
        joinedload(Site.alternate_domains),
        joinedload(Site.site_tags))
    msite = q.one_or_none()
    if msite is None:
        msite = Site(**site)
        for d in alternate_domains:
            mad = get_or_create_m(session, AlternateDomain, d, fb_uk='name')
            msite.alternate_domains.append(mad)
        for t in site_tags:
            mtag = get_or_create_m(session, SiteTag, t,
                                   fb_uk=['name', 'source'])
            msite.site_tags.append(mtag)
        session.add(msite)
    elif onduplicate == 'update':
        # UPDATE site
        session.query(Site).filter_by(id=msite.id).update(site)
        adding_domains = [d['name'] for d in alternate_domains]
        owned_domains = [mad.name for mad in msite.alternate_domains]
        # HANDLE ALTERNATE DOMAINS
        # delete non-need ones
        for mad in msite.alternate_domains:
            if mad.name not in adding_domains:
                session.delete(mad)
        # add new ones
        for d in alternate_domains:
            if d['name'] not in owned_domains:
                session.add(AlternateDomain(site_id=msite.id, **d))
        # HANDLE SITE TAGS
        adding_tags = [(t['name'], t['source']) for t in site_tags]
        owned_tags = [(mt.name, mt.source) for mt in msite.site_tags]
        # delete non-need ones
        for mt in msite.site_tags:
            if (mt.name, mt.source) not in adding_tags:
                session.delete(mt)
        # add new ones
        for t in site_tags:
            if (t['name'], t['source']) not in owned_tags:
                mtag = get_or_create_m(session, SiteTag, t,
                                       fb_uk=['name', 'source'])
                msite.site_tags.append(mtag)
    try:
        session.commit()
    except IntegrityError as e:
        logger.exception(e)
        session.rollback()


def convert_to_sqlalchemy_statement(raw_sql_script):
    """Convert raw SQL into SQLAlchemy statement."""
    # remove comment and tail spaces
    formated_sql_script = sqlparse.format(raw_sql_script.strip(),
                                          strip_comments=True)
    return sqlparse.split(formated_sql_script)


def migrate_db(sql_script):
    """Migrate database using sql_script."""
    logger.info('Start migration %r', __file__)
    with ENGINE.connect() as conn:
        conn.autocommit = True
        conn.execution_options(isolation_level='AUTOCOMMIT')
        for stmt in convert_to_sqlalchemy_statement(sql_script):
            logger.debug('Executing sql statement %r', stmt)
            conn.execute(stmt)
    logger.info('Migration finished!')


def column_windows(session, w_column, w_size, fb_kw=None, f_expr=None):
    """Return a series of WHERE clauses against a given column that break it
    into windows.

    Parameters
    ----------
    session : object
        An instance of SQLAlchemy Session.
    w_column : object
        Column object that is used to split into windows, should be an
        integer column.
    w_size : int
        Size of the window
    fb_kw : dict
        The filter_by keywords, used by query.filter_by().
    f_expr : list
        The filter expressions, used by query.filter().

    Returns
    -------
    iterable
        Each element of the iterable is a whereclause expression, which
        specify the range of the window over the column `w_col`.

    Exmaple
    -------
    for whereclause in column_windows(q.session, w_column, w_size):
        for row in q.filter(whereclause).order_by(w_column):
            yield row

    """
    def int_for_range(start_id, end_id):
        """Internal function to build range."""
        if end_id:
            return and_(
                w_column >= start_id,
                w_column < end_id
            )
        else:
            return w_column >= start_id
    q = session.query(w_column, func.row_number().over(
        order_by=w_column).label('w_row_num'))
    if fb_kw:
        q = q.filter_by(**fb_kw)
    if f_expr:
        q = q.filter(*f_expr)
    q = q.from_self(w_column)
    if w_size > 1:
        q = q.filter(sqlalchemy.text("w_row_num % {}=1".format(w_size)))

    intervals = [id for id, in q]

    while intervals:
        start = intervals.pop(0)
        if intervals:
            end = intervals[0]
        else:
            end = None
        yield int_for_range(start, end)


def get_platform_id(session, name):
    """A quick query to get the platform id by its name."""
    return session.query(Platform.id).filter_by(name=name).scalar()
