package grails.plugins.timeseries.redis

import grails.plugins.timeseries.*
import grails.converters.*
import groovy.transform.PackageScope
import redis.clients.jedis.*

class RedisTimeSeriesProvider extends AbstractTimeSeriesProvider {
	public RedisTimeSeriesProvider() {
		super()
	}


	@Override
	void manageStorage(groovy.util.ConfigObject config) {
		// select distinct metric names
	}

	@Override
	void init(groovy.util.ConfigObject config) {

	}

	@Override
	void shutDown(groovy.util.ConfigObject config) {
	}

	@Override
	void flush(groovy.util.ConfigObject config) {
		//getRedisService().flushDB()
		getRedisService().withPipeline {pipeline->
			def keys = pipeline.keys('m:*')
			pipeline.sync()
			keys.get().each {
				pipeline.del(it)
			}
			keys = pipeline.keys('c:*')
			pipeline.sync()
			keys.get().each {
				pipeline.del(it)
			}
			keys = pipeline.keys('ma:*')
			pipeline.sync()
			keys.get().each {
				pipeline.del(it)
			}
			keys = pipeline.keys('ca:*')
			pipeline.sync()
			keys.get().each {
				pipeline.del(it)
			}
			pipeline.del('km')
			pipeline.del('kc')
		}

	}

	void rebuildReferenceIdSets() {
		getRedisService().withPipeline {pipeline->
			def keys = pipeline.keys('m:*')
		}

	}


	@Override
	String getName() {
		return 'redis'
	}

	@Override
	String toString() {
		super.toString()
	}

	private getRedisService() {
		//grailsApplication.mainContext['elasticSearchHelper']
		grails.util.Holders.grailsApplication.mainContext['redisService']
	}

	// this needs to be redone with raw sql

	@Override
	void saveCounters(String referenceId, Map<String, Double> counters, Date timestamp, groovy.util.ConfigObject config) {
		def startAndInterval,
			aggregates,
			aggregateExpirations
	    getRedisService().withPipeline { Pipeline pipeline ->
			counters.each {k, v->
				startAndInterval = getCounterStartAndInterval(k, timestamp, config)
				def id = 'c:'+referenceId+':'+k+':'+startAndInterval.start.time,
					exp
		    	// Create Counter Definition
		    	pipeline.sadd('kc',referenceId+':'+k)
		    	pipeline.hincrByFloat(id, "${startAndInterval.interval}", v.floatValue())
		    	// Create Counter Definition for Hour Bucket
		    	if (getMillisecondExpirations(k, config)) {
		    		exp = (timestamp.time + getMillisecondExpirations(k, config))/1000
		    		pipeline.expireAt(id, exp.intValue())
		    	}
				aggregates = getCounterAggregateStartsAndIntervals(k, timestamp, config)
				aggregateExpirations = getCounterAggregateMillisecondExpirations(k,config)
				aggregates?.each {agg->
					def aggId = 'ca:'+referenceId+':'+k+':'+agg.resolution+':'+agg.start.time

			    	pipeline.hincrByFloat(aggId, "${agg.interval}", v.floatValue())
			    	// Create Counter Definition for Hour Bucket
			    	if (aggregateExpirations[agg.resolution]) {
			    		exp = (System.currentTimeMillis() + aggregateExpirations[agg.resolution])/1000
			    		pipeline.expireAt(aggId, exp.intValue())
			    	}
			    }
			}
		}
	}


	@Override
	void saveMetrics(String referenceId, Map<String, Double> metrics, Date timestamp, groovy.util.ConfigObject config) {
		def startAndInterval,
			aggregates,
			aggregateExpirations
	    getRedisService().withPipeline { Pipeline pipeline ->
			metrics.each {k, v->
				startAndInterval = getCounterStartAndInterval(k, timestamp, config)
				def id = 'm:'+referenceId+':'+k+':'+startAndInterval.start.time,
					exp
		    	// Create Counter Definition
		    	pipeline.sadd('km',referenceId+':'+k)
		    	pipeline.hset(id, "${startAndInterval.interval}", v.toString())
		    	// Create Counter Definition for Hour Bucket
		    	if (getMillisecondExpirations(k, config)) {
		    		exp = (timestamp.time + getMillisecondExpirations(k, config))/1000
		    		pipeline.expireAt(id, exp.intValue())
		    	}
				aggregates = getAggregateStartsAndIntervals(k, timestamp, config)
				aggregateExpirations = getAggregateMillisecondExpirations(k,config)
				aggregates?.each {agg->

					def aggId = 'ma:'+referenceId+':'+k+':'+agg.resolution+':'+agg.start.time

			    	pipeline.hset(aggId, "${agg.interval}", v.toString())
			    	// Create Counter Definition for Hour Bucket
			    	if (aggregateExpirations[agg.resolution]) {
			    		exp = (timestamp.time + aggregateExpirations[agg.resolution])/1000
			    		pipeline.expireAt(aggId, exp.intValue())
			    	}
			    }
			}
		}
	}

	@Override
	void bulkSaveCounters(String referenceId, List<Map<Date, Map<String, Double>>> countersByTime, groovy.util.ConfigObject config) {
		countersByTime.each {timestamp, counters->
			saveCounters(referenceId, counters, timestamp, config)
		}
	}

	@Override
	void bulkSaveMetrics(String referenceId, List<Map<Date, Map<String, Double>>> metricsByTime, groovy.util.ConfigObject config) {
		metricsByTime.each {timestamp, metrics->
			saveMetrics(referenceId, metrics, timestamp, config)
		}
	}

	@Override
	Map getCounters(Date start, Date end, String referenceIdQuery, String counterNameQuery, Map<String, Object> options, groovy.util.ConfigObject config) {
		def rtn = [start:start, end:end,  items: []]

		// get list of matching refIds
		def counterNames = [], referenceIds = [], counterDurations = [:]

	    getRedisService().withPipeline { Pipeline pipeline ->
			def kc = pipeline.smembers('kc')
			pipeline.sync()
			kc.get().each {k ->
				def parts = k.split(':')
				if (counterNameQuery == null || counterNameQuery == '*' || parts[1].matches(counterNameQuery)) {
					counterNames << parts[1]
				}
				if (referenceIdQuery == null || referenceIdQuery == '*' || parts[0].matches(referenceIdQuery)) {
					referenceIds << parts[0]
				}
			}

			if (counterNames.size() == 0 || referenceIds.size() == 0) return rtn
			def responseHash = [:],
			    id
			referenceIds.each {referenceId->
				responseHash[referenceId] = [:]
				counterNames.each {k->
					responseHash[referenceId][k] = [:]
					def startAndInterval = getCounterStartAndInterval(k, start, config)
					counterDurations[k] = ((long)startAndInterval.intervalSecs) * 1000l
					def tmpStart = startAndInterval.start.time
					while (tmpStart < end.time) {
						id = 'c:'+referenceId+':'+k+':'+tmpStart
						responseHash[referenceId][k][tmpStart] = pipeline.hgetAll(id)
						tmpStart += ((long)startAndInterval.range) * 1000l
					}
				}
			}
			pipeline.sync()
			responseHash.each {refId, counters->
				def series = []
				counters.each {counter, countsMap->
					def output = [name:counter, grandTotal: 0d, values:[]]
					countsMap.each {st, countsResp->
						def counts = countsResp.get()
						counts.each {interval, count->
							def lngInt = new Long(interval)
							def dur = counterDurations[counter]
							//println lngInt+'+'+dur
							output.grandTotal += new Double(count)
							output.values.add([start: new Date(new Long(st)  + (lngInt * dur)), count:new Double(count)])
						}
					}
					if (output.values.size() > 0) series.add(output)
				}
				counters.series = series

			}
			responseHash.each {k, v->
				def tmp = [referenceId: k, series:v.series]
				rtn.items.add(tmp)
			}
		}
	//		println new JSON(rtn).toString(true)
		rtn
	}


	@Override
	Map getMetrics(Date start, Date end, String referenceIdQuery, String metricNameQuery, Map<String, Object> options, groovy.util.ConfigObject config) {
		def rtn = [start:start, end:end,  items: []]

		// get list of matching refIds
		def counterNames = [], referenceIds = [], counterDurations = [:]

	    getRedisService().withPipeline { Pipeline pipeline ->
			def kc = pipeline.smembers('km')
			pipeline.sync()
			kc.get().each {k ->
				def parts = k.split(':')
				if (metricNameQuery == null || metricNameQuery == '*' || parts[1].matches(metricNameQuery)) {
					counterNames << parts[1]
				}
				if (referenceIdQuery == null || referenceIdQuery == '*' || parts[0].matches(referenceIdQuery)) {
					referenceIds << parts[0]
				}
			}

			if (counterNames.size() == 0 || referenceIds.size() == 0) return rtn
			def responseHash = [:],
			    id
			referenceIds.each {referenceId->
				responseHash[referenceId] = [:]
				counterNames.each {k->
					responseHash[referenceId][k] = [:]
					def startAndInterval = getMetricStartAndInterval(k, start, config)
					counterDurations[k] = ((long)startAndInterval.intervalSecs) * 1000l
					def tmpStart = startAndInterval.start.time
					while (tmpStart < end.time) {
						id = 'm:'+referenceId+':'+k+':'+tmpStart
						responseHash[referenceId][k][tmpStart] = pipeline.hgetAll(id)
						tmpStart += ((long)startAndInterval.range) * 1000l
					}
				}
			}
			pipeline.sync()
			responseHash.each {refId, counters->
				def series = []
				counters.each {counter, countsMap->
					def output = [name:counter, values:[], grandTotal:0d]
					countsMap.each {st, countsResp->
						def counts = countsResp.get()
						counts.each {interval, count->
							def lngInt = new Long(interval)
							def dur = counterDurations[counter]
							output.grandTotal += new Double(count)
							output.values.add([timestamp: new Date(new Long(st)  + (lngInt * dur)), value:new Double(count)])
						}
					}
					if (output.values.size() > 0) {
						series.add(output)
					}
				}
				counters.series = series
			}
			responseHash.each {k, v->
				def tmp = [referenceId: k, series:v.series]
				rtn.items.add(tmp)
			}
		}
		rtn
	}

	@Override
	Map getCounterAggregates(String resolution, Date start, Date end, String referenceIdQuery, String counterNameQuery, Map<String, Object> options, groovy.util.ConfigObject config) {
		def rtn = [start:start, end:end,  items: []]

		// get list of matching refIds
		def counterNames = [], referenceIds = [], counterDurations = [:]

	    getRedisService().withPipeline { Pipeline pipeline ->
			def kc = pipeline.smembers('kc')
			pipeline.sync()
			kc.get().each {k ->
				def parts = k.split(':')
				if (counterNameQuery == null || counterNameQuery == '*' || parts[1].matches(counterNameQuery)) {
					counterNames << parts[1]
				}
				if (referenceIdQuery == null || referenceIdQuery == '*' || parts[0].matches(referenceIdQuery)) {
					referenceIds << parts[0]
				}
			}

			if (counterNames.size() == 0 || referenceIds.size() == 0) return rtn
			def responseHash = [:],
			    id
			referenceIds.each {referenceId->
				responseHash[referenceId] = [:]
				counterNames.each {k->
					responseHash[referenceId][k] = [:]
					def startAndInterval = getCounterAggregateStartsAndIntervals(k, start, config).find {it.resolution == resolution}
					counterDurations[k] = ((long)startAndInterval.intervalSecs) * 1000l
					def tmpStart = startAndInterval.start.time
					while (tmpStart < end.time) {
						id = 'c:'+referenceId+':'+k+':'+resolution+':'+tmpStart
						responseHash[referenceId][k][tmpStart] = pipeline.hgetAll(id)
						tmpStart += ((long)startAndInterval.range) * 1000l
					}
				}
			}
			pipeline.sync()
			responseHash.each {refId, counters->
				def series = []
				counters.each {counter, countsMap->
					def output = [name:counter, grandTotal:0d, values:[]]
					countsMap.each {st, countsResp->
						def counts = countsResp.get()
						counts.each {interval, count->
							def lngInt = new Long(interval)
							def dur = counterDurations[counter]
							//println lngInt+'+'+dur
							output.grandTotal += new Double(count)
							output.values.add([start: new Date(new Long(st)  + (lngInt * dur)), count:new Double(count)])
						}
					}
					if (output.values.size() > 0) series.add(output)
				}
				counters.series = series

			}
			responseHash.each {k, v->
				def tmp = [referenceId: k, series:v.series]
				rtn.items.add(tmp)
			}
		}
	//		println new JSON(rtn).toString(true)
		rtn
	}


	@Override
	Map getMetricAggregates(String resolution, Date start, Date end, String referenceIdQuery, String metricNameQuery, Map<String, Object> options, groovy.util.ConfigObject config) {
		def rtn = [start:start, end:end,  items: []]

		// get list of matching refIds
		def counterNames = [], referenceIds = [], counterDurations = [:]

	    getRedisService().withPipeline { Pipeline pipeline ->
			def kc = pipeline.smembers('km')
			pipeline.sync()
			kc.get().each {k ->
				def parts = k.split(':')
				if (metricNameQuery == null || metricNameQuery == '*' || parts[1].matches(metricNameQuery)) {
					counterNames << parts[1]
				}
				if (referenceIdQuery == null || referenceIdQuery == '*' || parts[0].matches(referenceIdQuery)) {
					referenceIds << parts[0]
				}
			}

			if (counterNames.size() == 0 || referenceIds.size() == 0) return rtn
			def responseHash = [:],
			    id
			referenceIds.each {referenceId->
				responseHash[referenceId] = [:]
				counterNames.each {k->
					responseHash[referenceId][k] = [:]
					def startAndInterval = getAggregateStartsAndIntervals(k, start, config).find {it.resolution == resolution}
					counterDurations[k] = ((long)startAndInterval.intervalSecs) * 1000l
					def tmpStart = startAndInterval.start.time
					while (tmpStart < end.time) {
						id = 'ma:'+referenceId+':'+k+':'+resolution+':'+tmpStart
						responseHash[referenceId][k][tmpStart] = pipeline.hgetAll(id)
						tmpStart += ((long)startAndInterval.range) * 1000l
					}
				}
			}
			pipeline.sync()
			responseHash.each {refId, counters->
				def series = []
				counters.each {counter, countsMap->
					def output = [name:counter, values:[]]
					countsMap.each {st, countsResp->
						def counts = countsResp.get()
						counts.each {interval, count->
							def lngInt = new Long(interval)
							def dur = counterDurations[counter]
							output.values.add([timestamp: new Date(new Long(st)  + (lngInt * dur)), value:new Double(count)])
						}
					}
					if (output.values.size() > 0) {
						series.add(output)
					}
				}
				counters.series = series
			}
			responseHash.each {k, v->
				def tmp = [referenceId: k, series:v.series]
				rtn.items.add(tmp)
			}
		}
		rtn
	}


}