

class MetaParam:

    data_path        = ''
    transformed_path = ''
    table_name       = ''
    usecols          = ''

    def __init__(self, data_path, transformed_path, table_name, usecols ):
        try:
            self.data_path          = data_path
            self.transformed_path   = transformed_path
            self.table_name = table_name
            self.usecols            = usecols

        except Exception as e:
            print( 'MetaParams.__init__(), error: {}'.format( e ) )