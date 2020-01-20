from setuptools import setup

setup(
    name="etl_modules",
    install_requires=[
        "pyarrow~=0.15.1",
        "s3fs~=0.4.0"
    ]
) 