# @Author: Jason Hopper <hopperj>
# @Date:   2016-10-26
# @Email:  jason.t.hopper@gmail.com
# @Last modified by:   hopperj
# @Last modified time: 2016-11-11
# @License: GPL3


from setuptools import setup

setup(name='wow_auctions',
      version='0.1',
      description='A cli package to pull and manage wow auction data',
      setup_requires=['setuptools-markdown'],
    #   url='http://github.com/storborg/funniest',
      author='Jason Hopper',
      author_email='jason.t.hopper@gmail.com',
      license='GPL3',
      packages=['wow_auctions'],
      zip_safe=False,
      entry_points = {
          'console_scripts': ['wow_auctions=wow_auctions.cli:run'],
      },
      install_requires=[
        'click',
        'pymongo',
        'requests',
      ]
)
