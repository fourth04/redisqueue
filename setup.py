import setuptools

setuptools.setup(
    name="redisqueue",
    version="0.1.0",
    url="https://github.com/borntyping/cookiecutter-pypackage-minimal",

    author="gdgyy",
    author_email="GYangyun@gmail.com",

    description="一个基于redis的队列",

    packages=setuptools.find_packages(),

    install_requires=[],

    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
)
