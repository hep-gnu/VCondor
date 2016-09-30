from setuptools import setup

setup(name = 'vcondor',
    version = '0.1.0',
    license="'GPL3' or 'Apache 2'",
    install_requires=[
    "python>=2.6.0",
    ],
    description = "A dynamic virtual computing resource pool manager based HTCondor",
    author = "Haibo Li, Zhenjing Chen, Tao Cui,Yaodong Cheng",
    author_email = "ihp@ihep.ac.cn",
    url = "http://github.com/hep-gnu/vpmanager",
    packages = ['vcondor','vmquota'],
    data_files = [("/etc/", ["vcondor/VCondor.conf","vmquota/cloud.conf","vmquota/vmquota.conf"])],
    scripts = ["vcondor/VCondorMain.py","vmquota/VmquotaMain.py" ],
) 
