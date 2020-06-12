"""The setup script."""
from os.path import exists

from setuptools import find_packages, setup

from intake_civis._version import __version__ as version

readme = open("README.md").read() if exists("README.md") else ""


setup(
    name="intake-civis",
    description="Intake driver for Civis platform",
    long_description=readme,
    long_description_content_type="text/markdown",
    maintainer="Ian Rose",
    maintainer_email="ian.rose@lacity.org",
    url="https://github.com/CityOfLosAngeles/intake-civis",
    packages=find_packages(),
    package_dir={"intake-civis": "intake-civis"},
    include_package_data=True,
    install_requires=["civis", "intake"],
    extras_require={
        "geospatial": ["geopandas", "shapely"],
        "ibis": ["ibis-framework", "sqlalchemy", "sqlalchemy-redshift"],
    },
    entry_points={
        "intake.drivers": [
            "civis_cat = intake_civis.driver:CivisCatalog",
            "civis_schema = intake_civis.driver:CivisSchema",
            "civis = intake_civis.driver:CivisSource",
        ]
    },
    license="Apache-2.0 license",
    zip_safe=False,
    keywords="intake civis",
    version=version,
)
