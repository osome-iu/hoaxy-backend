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
    entry_points={
        'console_scripts': ['hoaxy = hoaxy.commands.cmdline:main']
    },
    classifiers=[
        'Framework :: Scrapy',
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: GPL :: Version 3',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    install_requires=[
        'botornot',
        'docopt>=0.6.0',
        'eventlet>=0.19.0',
        'Flask>=0.11.1',
        'gunicorn>=19.6',
        'networkx',
        'pandas',
        'psycopg2',
        'python-dateutil>=2.0',
        'pytz',
        'pyyaml',
        'schema',
        'scrapy>=1.3',
        'simplejson',
        'SQLAlchemy>=1.0',
        'sqlparse',
        'tabulate',
        'tweepy',
        'ruamel.yaml',
    ],
)
