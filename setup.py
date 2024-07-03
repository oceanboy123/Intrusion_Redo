from setuptools import setup, find_packages

setup(
    name='Intrusion_Redo',
    version='0.1',
    packages=find_packages(where="SRC"),
    package_dir={"":"SRC"},

    author='Edmundo David Garcia Larez',
    #author_email='eddg[at]me[dot]com',
    description='A Python tool to identify intrusion events in Bedford Basin, Halifax, NS, using BBMP BIO dataset',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/oceanboy123/Intrusion_Redo',
    py_modules=['Intrusion_analysis'],  # Assuming your script is named Intrusion_analysis.py
    install_requires=[
        # your dependencies here
    ],
    entry_points={
        'console_scripts': [
            'intrusion_analysis=Intrusion_analysis:main',  # This line sets up the command line tool
        ],
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    include_package_data=True,
    package_data={
        'Intrusion_analysis': ['../DATA/**/*']
    },
    python_requires='>=3.6',
)