from setuptools import setup
# pylint: disable=unspecified-encoding
with open('README.rst', 'r') as f:
    LONG_DESCRIPTION = f.read()

setup(
    name='pyxxl',
    version='0.1.3',
    description='A Python executor for XXL-jobs',
    long_description=LONG_DESCRIPTION,
    url='https://github.com/fcfangcc/pyxxl',
    packages=['pyxxl'],
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Quality Assurance',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    keywords=['xxl'],
    python_requires=">=3.8",
    install_requires=['aiohttp<4.0.0'],
)