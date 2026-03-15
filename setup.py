from setuptools import setup, find_packages

setup(
    name="stockanalyser",
    version="1.0.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.10",
    install_requires=[
        "pyspark==3.5.1",
        "requests==2.31.0",
        "python-dotenv==1.0.1",
        "pyarrow==16.1.0",
    ],
)
