from os.path import dirname, join
from setuptools import setup, find_packages

with open(join(dirname(__file__), 'hoaxy/VERSION'), 'rb') as f:
    version = f.read().decode('ascii').strip()

setup(
    name='hoaxy',
    version=version,
    url='http://cnets.indiana.edu',
    description='A framework to track online misinformation and its debunk',
    long_description=open('README.md').read(),
    author='Chengcheng Shao',
    maintainer='Chengcheng Shao',
    maintainer_email='shaoc@indiana.edu',
    license='GPLv3',
    packages=find_packages(exclude=('tests', 'tests.*')),
    include_package_data=True,
    zip_safe=True,
    entry_points={'console_scripts': ['hoaxy = hoaxy.commands.cmdline:main']},
    classifiers=[
        'Framework :: Scrapy',
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: GPL :: Version 3',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    install_requires=[
        'botometer',
        'docopt>=0.6.0',
        'eventlet>=0.19.0',
        'Flask>=0.11.1',
        'gunicorn>=19.9.0',
        'networkx==2.3',
        'pandas==0.24.2',
        'psycopg2>=2.7.6.1',
        'python-dateutil>=2.0',
        'pytz>=2019.1',
        'pyyaml>=5.1',
        'schema>=0.7.0',
        'scrapy>=1.3',
        'simplejson>=3.16.0',
        'SQLAlchemy>=1.0',
        'sqlparse>=0.2.4',
        'tabulate>=0.8.3',
        'tweepy>=3.7.0',
        'ruamel.yaml>=0.15.92',
        'newspaper3k==0.2.8',
        'pathos>=0.2.5',
        'demjson>=2.2.4',
        'xopen>=0.8.4'
    ],)
