from pymongo import Connection
from tweepy.streaming import StreamListener, Stream
from datetime import datetime
import web
render = web.template.render('templates/')
uri = 'mongodb://neilkod:startmongo@staff.mongohq.com:10055/earthquakes'


        
def mongo_connection(MONGO_URI):
	""" returns a mongoDB connection """
	connection = Connection(MONGO_URI)
	db = connection.earthquakes
	return db

def get_dynamic_count(term='earthquake'):
  connection = Connection(uri)
  print "....before: term is %s" % term
  db=connection.earthquakes
  if '/' in term:
    cleaned_term = term.split('/')[-1]
  else:
    cleaned_term = term
  print "....in dynamic count original term is %s" % cleaned_term
  conn = db.keyword_total
  foo = conn.find_one({'keyword':cleaned_term},{'count':1,'_id':0})
  #val = int(foo[0]['count'])
  try:
    val = int(foo['count'])
  except TypeError:
    val = 0
  connection.disconnect()
  print "....count is %s" % val
  return val 

def get_count():
  connection = Connection(uri)
  db=connection.earthquakes
  conn = db.quakes
  foo = conn.find_one({'name':'earthquake'},{'count':1,'_id':0})
  #val = int(foo[0]['count'])
  val = int(foo['count'])
  connection.disconnect()
  return val

urls = (
    '/data/(.*)', 'raw_data',
    '/data', 'raw_data',
    '/cnt/(.*)', 'get_cnt',
    '/cnt', 'get_cnt',
    '/(.*)', 'hello'

)
app = web.application(urls, globals())


class get_cnt:
  def GET(self, term='earthquake'):
    print "about to count for %s" % term
    bar = get_dynamic_count(term)
    return bar


class raw_data:
	def GET(self, term='earthquake'):
		# get a mongodb connection
		db = mongo_connection(uri)
		data = []
		
		result = db.keyword_period.find({'keyword':term},{'count':1,'period':1,'_id':0}, sort=[('period',-1)])
		#total_count = get_count(term)
		total_count = get_dynamic_count(term)
		print "count for %s is %s" % (term,total_count)
		for itm in result:
			# convert the period to a python date
			dt = datetime.strptime(itm['period'],'%Y%m%d%H%M')
			
			data.append((dt, itm['count']))

		
		params = [data, term, total_count]
		
		return render.data(params)


		
class hello:        
    def GET(self, name):
        bar = get_count()
        parms = [bar]
        if not name: 
            name = 'World'
        return render.index(parms)

if __name__ == "__main__":
    app.run()