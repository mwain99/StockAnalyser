from setuptools import setup, find_packages

setup(
    name="stockanalyser",
    version="1.0.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.10",
    install_requires=[
        "pyspark==3.5.8",
        "pydantic==2.12.5",
        "pydantic-settings==2.13.1",
        "pyarrow>=16.0.0",
        "requests==2.32.5",
        "python-dotenv==1.2.2",
    ],
)