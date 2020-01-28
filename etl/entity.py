'''
Entity will return a valid object representing data in a table/entity

    {
        name: string,
        data: dataframe | None
    }

'''


class Entity:

    tablename = ''
    dataframe = None

    def __init__(self, tablename, dataframe):
        self.tablename = tablename
        self.dataframe = dataframe

    def __repr__(self):
        return str(self.to_dict())

    def to_dict(self):
        return {
            'tablename': self.name,
            'dataframe': self.data
        }
