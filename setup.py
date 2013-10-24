from distutils.core import setup
#from distutils.command.install import INSTALL_SCHEMES
# from Cython.Distutils import build_ext
# from distutils.extension import Extension

#for scheme in INSTALL_SCHEMES.values():
#    scheme['data'] = scheme['purelib']

setup(
    name='medic',
    version='1',
    license='GNU GPL v3',
    author='Florian Leitner',
    author_email='florian.leitner@gmail.com',
    url='https://github.com/fnl/medic',
    description='a command line tool to manage a MEDLINE DB',
    long_description=open('README.rst').read(),
    install_requires=[
        'sqlalchemy >= 0.8',
        'psycopg2 >= 2.4',
        'nose',
    ],
    packages=[
        'medic',
    ],
    package_dir={ '': 'src' },
    scripts=[
        'scripts/medic',
    ],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
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
