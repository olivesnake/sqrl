from setuptools import setup, find_packages

VERSION = '0.1.0'
DESCRIPTION = 'A powerful and elegant sqlite API'
LONG_DESCRIPTION = ('a lightweight sqlite API that can do just about everything you need and allows you to fill in any '
                    'gaps')

setup(
    name="sqrl",
    version=VERSION,
    author="Oliver",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),
    install_requires=[],  # add any additional packages that
    # needs to be installed along with your package. Eg: 'caer'

    keywords=['python', 'sql', 'sqlite', 'database'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)
