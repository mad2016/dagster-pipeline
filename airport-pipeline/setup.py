from setuptools import find_packages, setup

setup(
    name="airport_pipeline",
    packages=find_packages(exclude=["airport_pipeline_tests"]),
    install_requires=["dagster", "dagster-cloud", "dagster-aws"],
    extras_require={"dev": ["dagit", "pytest"]},
)
