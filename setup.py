from setuptools import find_packages, setup

setup(
    name="covid19",
    packages=find_packages(exclude=["covid19_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
