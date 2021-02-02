import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="mwaa-rosbag-image-extraction",
    version="0.0.1",
    description="Rosbag image extraction using MWAA",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="hschoen@amazon.com",
    package_dir={"": "infrastructure"},
    packages=setuptools.find_packages(where="infrastructure"),
    install_requires=[
        "aws-cdk.core==1.83.0",
    ],
    python_requires=">=3.9",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.9",
        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",
        "Typing :: Typed",
    ],
)
