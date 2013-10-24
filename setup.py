from distutils.core import setup

setup(
    name='medic',
    version='1',
    license='GNU GPL v3',
    author='Florian Leitner',
    author_email='florian.leitner@gmail.com',
    url='https://github.com/fnl/medic',
    description='A command line tool to manage a PubMed DB mirror.',
    long_description=open('README.rst').read(),
    install_requires=[
        'sqlalchemy >= 0.8',
    ],
    py_modules=['medic'],
    packages=[
        'medic',
    ],
    package_dir={
        '': 'src',
    },
    scripts=[
        'scripts/medic',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'License :: OSI Approved :: GNU Affero General Public License v3',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 3',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'Topic :: Scientific/Engineering :: Medical Science Apps.',
        'Topic :: Software Development :: Libraries',
        'Topic :: Text Processing',
        'Topic :: Text Processing :: Linguistic',
    ],
)
