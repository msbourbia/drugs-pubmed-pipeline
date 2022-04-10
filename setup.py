from setuptools import setup

mypackage = 'drug_pubmed_pipeline'

setup(
    name=mypackage,
    version='0.0.1',
    packages=[mypackage],
    author='Mohamed-salah Bourbia',
    description='generates a graph showing the relation between the drugs and medical publications',
    url='https://github.com/msbourbia/drugs-pubmed-pipeline',
    install_requires=[
        'pandas==1.1.3',
        'ipython'
    ]
)