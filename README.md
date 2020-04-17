# Disclaimer

The name Hoaxy is a trademark of Indiana University. Neither the name "Hoaxy" nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

# Introduction

This document describes how to set up the hoaxy backend on your system.

[Hoaxy](http://hoaxy.iuni.iu.edu/) is a platform for tracking the diffusion of claims and their verification on social media. Hoaxy is composed of two parts, a Web app frontend, and a backend. This repository covers the backend part of Hoaxy, which currently supports tracking of social media shares from Twitter. To obtain the fronted, please go to:

    http://github.com/iunetsci/hoaxy-frontend

# Before Starting

## Python Environment

Hoaxy has been upgraded to use python3 under Ubuntu. We are beginning tests for Python 3.7.

The recommended installation method is to use a virtual environment. We recommend [anaconda](https://www.continuum.io/downloads) to setup a virtual environment. You *could* directly use the setuptools script by running `python setup.py install`, but that is not recommended if you are not an expert Linux user, as some dependencies (e.g. NumPy) need to be compiled and could result in compilation errors.

Anaconda provides already compiled packages for all dependencies needed to install Hoaxy. In the following, our instructions assume that you are using Anaconda. Here is an example of how to create and use python environment by conda.

1. Create a new python virtual environment, named hoaxy with version 3.7:

    ```bash
    conda create -n hoaxy python=3.7
    ```

2. activate it (note that before you use other python related command, you should activate your python environment first):

    ```bash
    source activate hoaxy
    ```
Most Linux distributions have installed their own python version. After the activation, you are using the new created python environment, which is separated from the system one. For the new created python environment, the actual python executable is located at `/ANACONDA_INSTALLATION_HOME/envs/ENV_NAME/bin/python`, where `ANACONDA_INSTALLATION_HOME` is the installation home of your anaconda, and `ENV_NAME` is the name of python environment (here is hoaxy). Please be aware that you must activate the python environment before you call any other python related command. 

## Lucene

Hoaxy uses [Apache Lucene](https://lucene.apache.org/) for indexing and searching. The python wrapper [pylucene](http://lucene.apache.org/pylucene/) is used to interface Hoaxy with Lucene. Unfortunately pylucene is neither avaiable via conda or pip, so you will have to compile it yourself.

1. [Download](http://www.apache.org/dyn/closer.lua/lucene/pylucene/) latest pylucene **7.6.0**(pylucene-7.6.0-src.tar.gz), the version we have tested.

2. Follow these [instructions](http://lucene.apache.org/pylucene/install.html) to compile and install pylucene. Please note that building the package is a time-consuming task. Also, _do not forget to activate the python environment_, otherwise pylucene will be installed under the system Python!

We found that the following tips made the compilation instructions of pylucene a bit easier to follow:
  - To build pylucene, you need gcc compiler. Recommended gcc version is GCC 5 or higher
  - If you are getting GCC related errors, add following exports in your shell
    - export JCC_ARGSEP=";"
    -  export JCC_CFLAGS="-v;-fno-strict-aliasing;-Wno-write-strings;-D__STDC_FORMAT_MACROS"
  - You can use `cd` instead of `pushd` and `popd`.
  - pylucene supports oracle jdk 1.8
  - pylucene needs apache ant 1.8.2 or higher
  - You will need the packages `default-jre` `default-jdk` `python-dev` and `ant` installed on the system (via apt-get if on Ubuntu).
  - ***Do not `sudo`*** anything during installation. Using sudo will prevent Lucene from being installed in the correct Anaconda and venv directories.
  - Two files need to be modified for your system, `setup.py` and `Makefile`. In these two files, the following three variables need to be set to reflect your installation and the virtual environment: `java`, `ant` and `python` (for the venv).

## PostgreSQL

Hoaxy use [PostgreSQL](https://www.postgresql.org/) to store all its data due to the possiblity of handling JSON data natively. Support for the JSON data type was introduced in version 9.3 of Postgres, but we recommend you use any version >=9.4 as it supports _binary_ JSON, or JSONB, a binary data type which results in significantly faster performance than the normal JSON type.

Please install and configure PostgreSQL. Once the database is ready, you need to create a user and a new database schema. To do so, connect to the DBMS with the `postgres` user:
```bash
sudo -u postgres psql
```

You will be taken to the interactive console of Postgres. Issue the following commands:
```sql
-- create a normal role, name 'hoaxy' with your own safe password 
CREATE USER hoaxy PASSWORD 'insert.your.safe.password.here';

-- alternatively you can issue the following command
CREATE ROLE hoaxy PASSWORD 'insert.your.safe.password.here' LOGIN;

-- create database, name 'hoaxy'
CREATE DATABASE hoaxy;

-- GIVE role 'hoaxy' the privileges to manage database 'hoaxy'
ALTER DATABASE hoaxy OWNER TO hoaxy;

-- Or you can grant all privileges of database 'hoaxy' to role 'hoaxy'
GRANT ALL PRIVILEGES ON DATABASE hoaxy TO hoaxy;
```

## Twitter Streaming API

Hoaxy tracks shares of claims and fact checking articles from the Twitter stream. To do so, it uses the [filter](https://dev.twitter.com/streaming/reference/post/statuses/filter) method of the [Twitter Streaming API](https://dev.twitter.com/streaming/overview). You must create at least one Twitter app authentication keys, and obtain their Access Token, Access Token Secret, Consumer Token and Consumer Secret information. Follow [these instructions](https://stackoverflow.com/questions/1808855/getting-new-twitter-api-consumer-and-secret-keys) to create a new app key and to generate all tokens. If you want to have the Botometer feature, you need another Twitter app authentication keys.

## Web Parser API

Hoaxy relies on two third-party libraries to parse and extract the content of Web documents. These libraries take care of removing all markup, as well as discarding all comments, ads, and site navigation text. 
The two libraries we use are, newspaper3k (https://newspaper.readthedocs.io/en/latest/) and Mercury(https://www.npmjs.com/package/@postlight/mercury-parser). Both of them are locally installed. 

For mercury parser, you need to install node first. Follow the instruction on https://nodejs.org/en/ to install node in your system. Then follow the instructions in  https://www.npmjs.com/package/@postlight/mercury-parser 
to install mercury parser in node. Copy the <hoaxy-backend>/hoaxy/node_scripts/parse_with_mercury.js to node_modules directory where mercury parser being installed. 


## Rapid API (_Optional_)

This is needed if you want to use the Web front end (see below) or if you want to provide a REST API with, among others, full-text search capabilities. Rapid API takes care of authentication and rate limiting, thus protecting your backend from heavy request loads. To set up Rapid API, user must create an account on the [Rapid API Marketplace](https://rapidapi.com) and create an API key.

## Botometer (_Optional_)

This is needed if you want to integrate [Botometer](http://botometer.iuni.iu.edu/) within Hoaxy to provide social bot scores for the most influential and most active accounts. The Botometer API is served via Rapid API and requires access to the Twitter REST API to fetch data about Twitter users. Botometer is integrated within Hoaxy through its Python bindings, see:

    https://github.com/IUNetSci/botometer-python


for more information.

## Web Front End (_Optional_)

If you want to show visualizations similar to the ones on the [official Hoaxy website](http://hoaxy.iuni.iu.edu/), then you should grab a copy of the hoaxy-frontend package at:

    http://github.com/iunetsci/hoaxy-frontend

If you want to use this system purely to collect data, this step is optional.

# Installation & Configuration Steps

These assume that all prerequisite have been satisfied (see above section).

1. Use conda to install all remaining dependencies (Remember: activate your python environment first):

    ```bash
    conda install docopt Flask gunicorn networkx pandas psycopg2 python-dateutil pytz pyyaml scrapy simplejson SQLAlchemy sqlparse tabulate
    ```
    Some of the packages are not official conda packages. You can use pip to install those packages.
    
    ```bash
    pip install tweepy ruamel.yaml newspaper3k demjson
    ```
    
2. Clone the hoaxy repository from Github:

    ```bash
    git clone git@github.com:IUNetSci/hoaxy-backend.git
    ```
    If you get an error about SSL certificates, you may need to set the environment variable `GIT_SSL_NO_VERIFY=1` temporarily to download the repo from github.

3. CD into the package folder:

    ```bash
    cd hoaxy-backend
    ```

4. If you are _not_ going to use Rapid API, you will need to edit the file `hoaxy/backend/api.py` to remove the `authenticate_rapidapi` decorator from the flask routes.

5. Install the package:

    ```bash
    python setup.py install
    ```

6. You can now set up hoaxy. A user-friendly command line interface is provided. For the full list of commands, type `hoaxy --help` from the command prompt.

7. Use the `hoaxy config` command to get a list of sample files.

    ```bash
    hoaxy config [--home=YOUR_HOAXY_HOME]
    ```
    The following sample files will be generated and placed into the configuration folder (default: `~/.hoaxy/`) with default values:

      - `conf.sample.yaml`
        
        The main configuration file.

      - `domains_claim.sample.txt`
      
        List of domains of claim websites; this is a simpler option over `sites.yaml`.

      - `domains_factchecking.sample.txt`
      
        List of domains of fact-checking websites; this is a simpler option over `sites.yaml`.

      - `sites.sample.yaml`
      
        Configuration of all domains to track. Allows for a fine control of all crawling options.

      - `crontab.sample.txt`
      
        Crontab to automate backend operation via the Cron daemon.
    
    By default, all configuration files will go under `~/.hoaxy/` unless you set the `HOAXY_HOME` environment variable or pass the `--home` switch to `hoaxy config`.
    
    If you get an error while running `hoaxy config`, you can simply go under `hoaxy/data/samples` and manually copy its contents to your `HOAXY_HOME`. Make sure to remove the `.sample` part from the extension (e.g. `conf.sample.yaml` -> `conf.yaml`).

8. Rename these sample files.  Ex: rename `conf.sample.yaml` to `conf.yaml`.

9. Configure Hoaxy for your needs. You may want to edit at least the following files:
      - `conf.yaml` is the main configuration file. 
      
        Search for `*** REQUIRED ***` in conf.yaml to find settings that must be configured, including database login information, Twitter access tokens, mercury parser locations etc.
      
      - `domains_claim.txt`, `domains_factchecking.txt` and `sites.yaml` are site data files, which specify which domains need to be tracked. 
      
        The `domains_*` files offer a simple way to specify sites, each line in these files is a domain which is the primary domain of the site. If you want finer control of the sites, you can provide `sites.yaml` file. Please check the [`sites.yaml` manual](./hoaxy/data/manuals/sites.readme.md) 
      
      - `crontab.txt` is the input for automating all tracking operation via Cron. 
        
        Please check [`crontab` manual](http://man7.org/linux/man-pages/man5/crontab.5.html) for more information on Cron.

10. Finally, initialize all database tables and load the information on the sites you want to track:

    ```bash
    hoaxy init
    ```

# How to Start the Backend for the First Time

Please follow these steps to start all Hoaxy backend services. Remember to run these only after activating the virtual environment!

***Note***: The order of these steps is important! You need to fetch the articles before running the Lucene index, and you need the index before starting the API; this last step is only needed if you want to enable the REST API for searching.

1. Fetch _only the latest_ article URLs:

    ```bash
    hoaxy crawl --fetch-url --update
    ```
    This will collect only the latest articles from specified domains.

2. (_Optional_) Fetch _all_ article URLs:

    ```bash
    hoaxy crawl --fetch-url --archive
    ```
    This will do a deep crawl of all domains to build a comprehensive archive all articles available on the specified files.
    
    ***Note***: This is a time consuming operation!

3. Fetch the body of articles:

    ```bash
    hoaxy crawl --fetch-html
    ```
    You may pass `--limit` to avoid making this step too time consuming when automating via cron.

4. Parse articles via the Mercury API:

    ```bash
    hoaxy crawl --parse-article
    ```
    You may pass `--limit` to avoid making this step too time consuming when automating via cron.

5. Start streaming from Twitter:

    ```bash
    hoaxy sns --twitter-streaming
    ```
    This is a non-interactive process and you should run it as a background service.

6. Build the Lucene index

    ```bash
    hoaxy lucene --index
    ```

7. (_Optional_) Run the API:

    ```bash
    # Set these to sensible values
    HOST=localhost
    PORT=8080
    gunicorn -w 2 --timeout=120 -b ${HOST}:${PORT} --error-logfile gunicorn_error.log hoaxy.backend.api:app
    ```

# Automated Deployment

After you have run the backend for the first time, Hoaxy will be ready to track new articles and new tweets. The following steps are needed if you want to run the backend in a fully automated fashion. Hoaxy needs to perform three kinds of tasks:

1. Cron tasks. These are periodic tasks, like crawling the RSS feed of a website, fetch newly collected URLs, and parsing the articles of newly collected URLs. To do so, you need to install the crontab for Hoaxy. The following will install a completely new crontab (i.e. it will replace any existing crontab):

    ```bash
    crontab crontab.txt
    ```
    
    ***Note***: we recommend to be careful with the capabilty of your crawling processes. Depending on the speed of your Internet connection you will wat to use the `--limit` option when calling the crawling commands:

    ```bash
    hoaxy crawl --fetch-html --limit=10000
    ```
    
    The above for example limits to fetching only 10,000 articles per hour (the default in the crontab). You will need to edit the `crontab.txt` and reinstall it for this change to take place.

2. Real-time tracking tasks. These include collecting tweet from the Twitter Streaming API. After the process is started, it will keep running. To manage this process, we recommend to use [supervisor](http://supervisord.org/). The following is an example configuration of supervisor (please replace all uppercase variables with sensible values):
    
    ```
    [program:hoaxy_stream]
    directory=/PATH/TO/HOAXY
    # You can add your path environment here
    # Example, /home/USER/anaconda3/envs/hoaxy/bin
    environment=PATH=PYTHON_BIN_PATH:%(ENV_PATH)s
    command=hoaxy sns --twitter-streaming
    user=USER_NAME
    stopsignal=INT
    stdout_logfile=NONE
    stderr_logfile=NONE
    ; Use the following when this switches to being served by gunicorn, or if the
    ; task can't be restarted cleanly
    ; killasgroup=true
    ; set autorestart=true for any exitcode
    ; autorestart=true
    ```

3. Hoaxy API. We recommand to use `supervisor` to control this process too (please replace all upercase variables with sensible values):
    
    ```
    [program:hoaxy_backend]
    directory=/PATH/TO/HOAXY
    environment=PATH=PYTHON_BIN_PATH:%(ENV_PATH)s
    command=gunicorn -w 6 --timeout=120 -b HOST:PORT --error-logfile gunicorn_error.log hoaxy.backend.api:app
    user=USER_NAME
    stderr_logfile=NONE
    stdout_logfile=NONE
    ; Use the following when this switches to being served by gunicorn, or if the
    ; task can't be restarted cleanly
    killasgroup=true
    stopasgroup=true
    ```
    
# Hoaxy backend as AWS image (AMI)
We installed hoaxy backend (pylucene, postgres, hoaxy repository, configuration, supervisor, conda etc) as an AWS AMI which you can 
use to host it in AWS. Here are the steps how to access AWS AMI.

1. Log in AWS using your aws credentials 
2. Go to Services -> EC2 and click on Launch Instance
3. Click on "Community AMIs" and search for "hoaxy" and you will find an AMI as "hoaxy-python3-ami - ami-0ab100889ff9a7530
"
4. Select that and follow the instructions to create instance using that AMI
5. Once you start the instance, you can find the hoaxy configuration under .hoaxy folder at home. You need to 
update the configuration with your twitter keys and rapid api token
 

# Frequently Asked Questions

## Do you have a general overview of the Hoaxy architecture?

Please check the [hoaxy system architecture](./hoaxy/data/manuals/architecture.readme.md) in the documentation. You can also see the early prototype of the Hoaxy system presented in the following paper:
```
@inproceedings{shao2016hoaxy,
  title={Hoaxy: A platform for tracking online misinformation},
  author={Shao, Chengcheng and Ciampaglia, Giovanni Luca and Flammini, Alessandro and Menczer, Filippo},
  booktitle={Proceedings of the 25th International Conference Companion on World Wide Web},
  pages={745--750},
  year={2016},
  organization={International World Wide Web Conferences Steering Committee}
}
```

## Can I specify a sub-domain to track from Twitter (e.g. foo.website.com)?

Hoaxy works by filtering from the full Twitter stream only those tweets that contain URL of specific domains. If you specify a website as, e.g. `www.domain.com`, the `www.` part will be automatically discarded. Likewise, any subdomain (e.g. `foo.website.com`) will be discarded too. This limitation is due to the way the filter endpoint of the Twitter API works. 

## Can I specify a specific path to track from Twitter (e.g. website.com/foo/)?

For the same reason why it cannot track sub-domains, Hoaxy cannot track tweets sharing URLs with a specific path (e.g. domain.com/foobar/) either.

However, when it comes to crawling domains, Hoaxy allows a fine control of the type of Web documents to fetch, and it is possible to crawl only certain parts of a website. Please refer to the `sites.yaml` configuration file.

## Can I specify alternate domains for the same website?

Most sites are accessible from just one domain, and often the domain reflects the colloquial site name. However, there are cases when the same site can be accessed from multiple domains. For example, the claim site [DC Gazette](http://thedcgazette.com/), owns two domains `thedcgazette.com` and `dcgazette.com`. When you make an HTTP request to `dcgazette.com`, it will be redirected to `thedcgazette.com`.

Thus we call `thedcgazette.com` the _primary_ domain and `dcgazette.com` the _alternate_. You provide the primary domain of a site, and alternate domains are optional. This is because when crawling we need to know the scope of our crawling, which is constrained by domains.

## How does Hoaxy crawl news articles?

Crawling of articles happens over three stages:

1. Collecting URLs.

    URLs are crawled as a result of two separate processes: first, tweets matching the domains we are monitoring; second, when we fetch new articles from news sites. The corresponding commands are:
    ```bash
    hoaxy sns --twitter-streaming
    ```
    for collecting URLs from tweets, and:
    ```bash
    hoaxy crawl --fetch-url (--update | --archive)
    ```
    for fetching URLs from RSS feeds and/or direct crawling (either with `--update` or `--archive` option),

2. Fetch the HTML.

    At this stage, we try to fetch the raw HTML page of all collected URLs. Short URLs (e.g. bit.ly) are also resolved at this time. To resolve any duplication issue, we use the "canonical" form of a URL to represent a bunch of URLs that refer to the same page. URL parameters are kept, with the exception of the ones starting with `utm_*`, which are used by Google Analytics. The corresponding command is:
    ```bash
    hoaxy crawl --fetch-html
    ```

3. Parsing the HTML. 

    At this stage, we try to extract the article text from HTML documents. Hoaxy relies on [a third-party API service](http://mercury.postlight.com/) to do so. You may want to implement your own parser, or use an already existing package (e.g. python-goose). The corresponding command is:
    ```bash
    hoaxy crawl --parse-article
    ```

## How does Hoaxy treat different tweet types (e.g. retweet, replies)?

Currently Hoaxy can only monitor one platform, Twitter. In Twitter there are different types of tweet for different behavior, e.g., retweet or reply. To identify the type of tweet, Hoaxy employs a simple set of heuristics. Please see the [`types of tweet` manual](./hoaxy/data/manuals/tweet.readme.md).
