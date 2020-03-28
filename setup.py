from os import path

from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))
about = {}
with open(path.join(here, 'nameko_kafka', 'version.py'), mode='r', encoding='utf-8') as f:
    exec(f.read(), about)

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

if __name__ == '__main__':
    setup(
        name='nameko_kafka',
        description='Kafka extension for Nameko microservice framework',
        keywords=['nameko', 'kafka', 'microservice'],
        version=about['__version__'],
        author='Ketan Goyal',
        author_email='ketangoyal1988@gmail.com',
        license="MIT",
        url='https://github.com/ketgo/nameko-kafka',
        long_description=long_description,
        long_description_content_type='text/markdown',
        py_modules=['nameko_kafka'],
        package_dir={'nameko_kafka': 'nameko_kafka'},
        python_requires='>=3.4',
        install_requires=[
            'nameko',
            'kafka-python',
            'wrapt==1.11'  # Fixed version: needed by kafka-python
        ],
        extras_require={
            'dev': [
                'pytest',
                'pytest-cov',
                'pytest-mock',
                'pylint',
            ]
        },
        packages=find_packages(exclude=('tests',)),
        classifiers=[
            'Intended Audience :: Developers',
            'License :: OSI Approved :: Apache Software License',
            'Operating System :: OS Independent',
            "Topic :: Internet",
            "Topic :: Software Development :: Libraries :: Python Modules",
            'Natural Language :: English',
            'Programming Language :: Python',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
        ],
    )
