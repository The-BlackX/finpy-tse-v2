import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='finpy_tse',
    packages=['finpy_tse'],
    version='1.2.2', 
    license='BSD (3-clause)',
    description='A Python Module to Access Tehran Stock Exchange Historical and Real-Time Data',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='ALI RAHIMI  AND  RASOOL GHAFOURI',
    author_email='a.rahimi.aut@gmail.com',
    install_requires=['requests', 'jdatetime', 'pandas', 'numpy', 'bs4', 'asyncio', 'urllib3', 'aiohttp', 'unsync', 'IPython', 'persiantools', 'datetime', 'XlsxWriter', 'lxml'],
    package_data={
        'finpy_tse': ['data/usd_history.csv'],
    },
    include_package_data=True,
)