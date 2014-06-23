class TimeSeriesRedisGrailsPlugin {
//    def groupId = "org.grails.plugins"

    def version = "0.1.4"
    def grailsVersion = "2.0 > *"
    def title = "Time Series Redis Plugin"
    def author = "Jeremy Leng"
    def authorEmail = "jleng@bcap.com"
    def description = 'Redis implementation of time series.'
    def dependsOn = [timeSeries: "* > 0.2-SNAPSHOT"]
    def documentation = "https://github.com/bertramdev/timeseries-redis"
    def license = "APACHE"
    def organization = [ name: "BertramLabs", url: "http://www.bertramlabs.com/" ]
    def issueManagement = [ system: "GIT", url: "https://github.com/bertramdev/timeseries-redis" ]
    def scm = [ url: "https://github.com/bertramdev/timeseries-redis" ]
    def doWithApplicationContext = { ctx ->
        ctx['timeSeriesService'].registerProvider(new grails.plugins.timeseries.redis.RedisTimeSeriesProvider())
    }
}
