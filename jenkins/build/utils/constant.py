# data talytalyrond
TALYROND_PATH = '/data/talyrond_ng' #'/data/talyrond_ng' ,'D:/data/talyrond_ng'
TALYROND_TABLE = 'data_talyrond'
TALYROND_TABLE_COLUMNS ='''
            registered_at datetime,
            parts_no varchar(20),
			lot_no varchar(10),
            entry_date datetime,
            mc_no varchar(10),
            measurement_item varchar(50),
            measurement_value float,
            mi_no varchar(10),
            individual_judgement varchar(5),
            checked varchar(10),
            checked_time datetime
            '''
            
TALYROND_TABLE_LOG = 'log_talyrond'
TALYROND_TABLE_COLUMNS_LOG ='''
            registered_at datetime,
			status varchar(50),
            file_name varchar(50),
            process varchar(50),
            message varchar(50),
            error varchar(MAX),
            '''

#LOG status
STATUS_OK = 'ok'
STATUS_ERROR = 'error'
STATUS_INFO = 'info'