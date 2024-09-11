from setuptools import setup, find_packages

setup(
    name="setuppy-whl",
    version="0.0.1",
    packages=find_packages(exclude=["tests", "*tests.*", "*tests"]),
    python_requires=">=3.7",
    author="John Doe",
    author_email="john@example.com",
)
