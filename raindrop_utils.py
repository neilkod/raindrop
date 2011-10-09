def get_mongo_uri(config_file = CONFIG_FILE):
	mongo_params = {}
	config = ConfigParser.ConfigParser()
	config.readfp(open(config_file))

	mongo_opts = dict(config.items('mongodb'))
	mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % \
		(mongo_opts['username'], mongo_opts['password'], mongo_opts['host'],
		mongo_opts['port'], mongo_opts['db'])	
	return mongo_uri
