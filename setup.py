"""
This is the setup.py file for the unofficial Surreal API wrapper. This is a wrapper for the Surreal API.
"""
import sys

# # For now, this project only support Python 3.9 and above.
# if sys.version_info < (3, 9):
#     sys.exit("Python 3.9 or above is required.")

from setuptools import setup, find_packages
import pathlib
from surrealpy import __version__, __authors__

here = pathlib.Path(__file__).parent.resolve()

long_description = (here / "README.md").read_text(encoding="utf-8")


setup(
    name="surrealpy",
    version=__version__,
    description="Unofficial Surreal API wrapper",
    long_description_content_type="text/markdown",
    author="bilinenkisi, redbuls81",
    author_email="berkayozbay64@gmail.com",
    url="https://vulnware.com/projects/surrealpy",
    # requests is required for the HTTP client but in the future, I will change it to httpx. It is same for aiohttp.
    install_requires=["requests", "websocket_client", "aiohttp", "psutil"],
    license="MIT",
    long_description=long_description,
    classifiers=[  # Optional
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        "Development Status :: 3 - Alpha",
        # Indicate who your project is intended for
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Api Wrapper",
        # Pick your license as you wish
        "License :: OSI Approved :: MIT License",
        # Specify the Python versions you support here. In particular, ensure
        # that you indicate you support Python 3. These classifiers are *not*
        # checked by 'pip install'. See instead 'python_requires' below.
        # "Programming Language :: Python :: 3",
        # "Programming Language :: Python :: 3.7",
        # "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3 :: Only",
    ],
    keywords="surrealDB, surrealDB api wrapper, surrealDB client, Database",
    packages=["surrealpy"],
    python_requires=">=3.9, <4",
    project_urls={  # Optional
        "Bug Reports": "https://github.com/Vulnware/SurrealPy/issues",
        "Source": "https://github.com/Vulnware/SurrealPy",
    },
)
