from setuptools import setup, find_namespace_packages
  
install_requires=["pyyaml", "pika"]

setup(
    name="dm_csc_base",
    version="1.0.0",
    description="dm_csc base classes",
    install_requires=install_requires,
    package_dir={"": "python"},
    packages=find_namespace_packages(where="python"),
    scripts=["bin/csc_command.py"],
    license="GPL",
    zip_safe=False,
    project_urls={
        "Bug Tracker": "https://jira.lsstcorp.org/secure/Dashboard.jspa",
        "Source Code": "https://github.com/lsst-dm/dm_csc_base",
    }
)
