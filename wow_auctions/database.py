# @Author: Jason Hopper <hopperj>
# @Date:   2016-11-26
# @Email:  hopperj@ampereinnotech.com
# @Last modified by:   hopperj
# @Last modified time: 2016-11-26
# @License: GPL3


def get_database(db_uri):

    connection = pymongo.MongoClient(
        db_uri
    )


    db = connection[db_name]

    yield db
    # urls_collection = db[db_url_name]
    # data_collection = db[db_data_name]
