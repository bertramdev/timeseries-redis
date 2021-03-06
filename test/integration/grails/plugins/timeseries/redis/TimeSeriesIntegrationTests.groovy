package grails.plugins.timeseries.redis

import groovy.util.GroovyTestCase
import grails.converters.*
import grails.plugins.timeseries.*

class TimeSeriesIntegrationTests extends GroovyTestCase {
	def transactional =  false
	def timeSeriesService
	def grailsApplication
	def setup() {
	}

	def cleanup() {
	}

	private getTestDate() {
		def c = new GregorianCalendar()
		c.set( Calendar.ERA, GregorianCalendar.AD )
		c.set( Calendar.YEAR, 2013 )
		c.set( Calendar.MONTH, Calendar.OCTOBER )
		c.set( Calendar.DATE, 31 )
		c.set( Calendar.HOUR, 10 )
		c.set( Calendar.SECOND, 31 )
		c.set( Calendar.MINUTE, 31 )
		c.set( Calendar.MILLISECOND, 0 )
		c.time
	}
	void testSaveCounter() {
		timeSeriesService.flush()
		def now = getTestDate()
		println now
		grailsApplication.config.grails.plugins.timeseries.counters.poop.resolution = AbstractTimeSeriesProvider.ONE_MINUTE
		5.times {
			timeSeriesService.saveCounter('testSaveCounter', 'poop', 1d, now)
		}
		Thread.sleep(1000)
		println new JSON(timeSeriesService.getCounters(now - 1, new Date(System.currentTimeMillis() + 180000l), 'testSaveCounter', 'poop')).toString(true)
	}

	void testSaveMetrics() {
		timeSeriesService.flush()
		def now = getTestDate()
		println now
		grailsApplication.config.grails.plugins.timeseries.poop.resolution = AbstractTimeSeriesProvider.ONE_SECOND
		timeSeriesService.saveMetric('testSaveMetrics', 'poop', 100d, now)
		println new JSON(timeSeriesService.getMetrics(now-1, new Date(System.currentTimeMillis() + 180000l), 'testSaveMetrics')).toString(true)
	}

	void testSaveCounterWithHourlyAggregate() {
		timeSeriesService.flush()
		def now = getTestDate()
		println now
		grailsApplication.config.grails.plugins.timeseries.poop.expiration = 631138519494l
		grailsApplication.config.grails.plugins.timeseries.counters.poop.resolution = AbstractTimeSeriesProvider.ONE_MINUTE
		grailsApplication.config.grails.plugins.timeseries.counters.poop.aggregates = ['1h':'1d']
		5.times {
			timeSeriesService.saveCounter('testSaveCounterWithHourlyAggregate', 'poop', 1d, now)
		}

		now = new Date(now.time + (61000l*60l))
		5.times {
			timeSeriesService.saveCounter('testSaveCounterWithHourlyAggregate', 'poop', 1d, now)
		}
		Thread.sleep(1000)
		println new JSON(timeSeriesService.getCounterAggregates('1h',getTestDate()- 1, new Date(System.currentTimeMillis() + 180000l), 'testSaveCounterWithHourlyAggregate', 'poop')).toString(true)
	}


	void testSaveMetricsWithHourlyAggregate() {
		timeSeriesService.flush()
		def now = getTestDate()
		println now
		grailsApplication.config.grails.plugins.timeseries.poop.expiration = 631138519494l
		grailsApplication.config.grails.plugins.timeseries.poop.resolution = AbstractTimeSeriesProvider.ONE_SECOND
		grailsApplication.config.grails.plugins.timeseries.poop.aggregates = ['1h':'10000d']
		timeSeriesService.saveMetric('testSaveMetrics', 'poop', 100d, now)
		println new JSON(timeSeriesService.getMetricAggregates('1h',now - 1, new Date(System.currentTimeMillis() + 180000l), 'testSaveMetrics')).toString(true)
	}


	void testSaveMetricsOverwrite() {
		timeSeriesService.flush()
		def now = getTestDate()
		println now
		grailsApplication.config.grails.plugins.timeseries.poop.expiration = 631138519494l
		grailsApplication.config.grails.plugins.timeseries.poop.resolution = AbstractTimeSeriesProvider.ONE_SECOND
		timeSeriesService.saveMetric('testSaveMetricsOverwrite', 'poop', 100d, now)
		timeSeriesService.saveMetric('testSaveMetricsOverwrite', 'poop', 200d, now)
		println new JSON(timeSeriesService.getMetrics(now - 1, new Date(System.currentTimeMillis() + 180000l), 'testSaveMetricsOverwrite')).toString(true)
	}


	void testSaveMetricsIrregular() {
		timeSeriesService.flush()
		def now = getTestDate()
		grailsApplication.config.grails.plugins.timeseries.poop.expiration = 631138519494l
		grailsApplication.config.grails.plugins.timeseries.poop.resolution = AbstractTimeSeriesProvider.ONE_SECOND
		[11,17,5,7].each {
			//println now
			timeSeriesService.saveMetric('testSaveMetricsIrregular', 'poop', it, now)
			now = new Date(now.time + (it*1000))
		}
		println new JSON(timeSeriesService.getMetrics(getTestDate() - 1, new Date(System.currentTimeMillis() + 180000l), 'testSaveMetricsIrregular')).toString(true)
	}

	void testSaveMetricsRegular() {
		timeSeriesService.flush()
		def now = getTestDate()
		grailsApplication.config.grails.plugins.timeseries.poop.expiration = 631138519494l
		grailsApplication.config.grails.plugins.timeseries.poop.resolution = AbstractTimeSeriesProvider.ONE_SECOND
		(1..15).each {
			//println now
			timeSeriesService.saveMetric('testSaveMetricsRegular', 'poop', it, now)
			now = new Date(now.time + 1000)
		}
		println new JSON(timeSeriesService.getMetrics(getTestDate() - 1, new Date(System.currentTimeMillis() + 180000l), 'testSaveMetricsRegular')).toString(true)
	}
	void testSaveMetricsRegularWithAggregates() {
		timeSeriesService.flush()
		grailsApplication.config.grails.plugins.timeseries.met1.aggregates = ['1m':'10000d']
		def now = getTestDate()
		grailsApplication.config.grails.plugins.timeseries.met1.expiration = 631138519494l
		grailsApplication.config.grails.plugins.timeseries.met1.resolution = AbstractTimeSeriesProvider.ONE_SECOND
		(1..121).each {
			//println now
			timeSeriesService.saveMetric('testSaveMetricsRegularWithAggregates', 'met1', it, now)
			now = new Date(now.time + 1000)
		}
		println new JSON(timeSeriesService.getMetricAggregates('1m',getTestDate() - 1, new Date(System.currentTimeMillis() + 180000l), 'testSaveMetricsRegularWithAggregates')).toString(true)
	}


	void testSaveMetricsRegularWithGet() {
		timeSeriesService.flush()
		def now = getTestDate()
		grailsApplication.config.grails.plugins.timeseries.met1.expiration = 631138519494l
		grailsApplication.config.grails.plugins.timeseries.met1.resolution = AbstractTimeSeriesProvider.ONE_SECOND
		grailsApplication.config.grails.plugins.timeseries.met2.expiration = 631138519494l
		grailsApplication.config.grails.plugins.timeseries.met2.resolution = AbstractTimeSeriesProvider.ONE_SECOND
		(1..35).each {
			//println now
			timeSeriesService.saveMetrics('testSaveMetricsRegularWithGet', ['met1':it, 'met2':(121-it)], now)
			now = new Date(now.time + 1000)
		}
		println new JSON(timeSeriesService.getMetrics(getTestDate() - 1, new Date(System.currentTimeMillis() + 180000l), 'testSaveMetricsRegularWithGet')).toString(true)
	}

	void testSaveMetricsRegularWithAggregatesWithGet() {
		timeSeriesService.flush()
		grailsApplication.config.grails.plugins.timeseries.met1.expiration = 631138519494l
		grailsApplication.config.grails.plugins.timeseries.met2.expiration = 631138519494l
		grailsApplication.config.grails.plugins.timeseries.met1.aggregates = ['1m':'10000d']
		grailsApplication.config.grails.plugins.timeseries.met2.aggregates = ['1m':'10000d']
		grailsApplication.config.grails.plugins.timeseries.met1.resolution = AbstractTimeSeriesProvider.ONE_SECOND
		grailsApplication.config.grails.plugins.timeseries.met2.resolution = AbstractTimeSeriesProvider.ONE_SECOND
		def now = getTestDate()
		(1..121).each {
			//println now
			timeSeriesService.saveMetrics('testSaveMetricsRegularWithAggregatesWithGet', ['met1':it, 'met2':(121-it)], now)
			timeSeriesService.saveMetrics('testSaveMetricsRegularWithAggregatesWithGet2', ['met1':it*1.23, 'met2':(121-it)*1.23], now)
			now = new Date(now.time + 1000)
		}
		println new JSON(timeSeriesService.getMetricAggregates('1m',getTestDate() - 1, new Date(System.currentTimeMillis() + 180000l),'testSaveMetricsRegularWithAggregatesWithGet')).toString(true)
	}

	void testManageStorage() {
		timeSeriesService.flush()
		grailsApplication.config.grails.plugins.timeseries.met1.resolution = AbstractTimeSeriesProvider.ONE_HOUR
		grailsApplication.config.grails.plugins.timeseries.met1.expiration = '2d'
		grailsApplication.config.grails.plugins.timeseries.met1.aggregates = ['1d':'2d']
		def now = new Date(),
			old = now - 5,
			it = 1
		while (true) {
//			println 'Saving '+old
			timeSeriesService.saveMetric('testManageStorage', 'met1', it, old)
			old = new Date(old.time + 3600000l)
			if (old > now) break
			it++
		}
		println new JSON(timeSeriesService.getMetrics(getTestDate() - 6, new Date(System.currentTimeMillis() + 180000l))).toString(true)
		timeSeriesService.manageStorage()
		println new JSON(timeSeriesService.getMetrics(getTestDate() - 6, new Date(System.currentTimeMillis() + 180000l))).toString(true)
	}

}

