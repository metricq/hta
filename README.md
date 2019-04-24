[![Build Status](https://travis-ci.com/metricq/hta.svg?branch=master)](https://travis-ci.com/metricq/hta)

Hierarchical Timeline Aggregation
=================================

Hierarchical Timeline Aggregation (or short HTA) is reference library for a file format tailored towards a fast store and load of metric data.

To speed up the access, it comprises raw data as well as aggregated information organized in plain files. The raw data is stored as timestamp-value pairs. The aggregates comprise values for the count, minimum, maximum, sum and integral over a given time interval. A hierarchy organizes the aggregates, where an aggregate of one hierarchy level includes several aggregates of the subsequent level. Each hierarchy level aggregates the previous level in intervals with a constant duration within the same level. The relation between the span of the interval of two consecutive hierarchy levels is called the *interval factor*. The interval factor is constant over all levels within the hierarchy for one specific metric.  The interval duration of the first and the last hierarchy level is called *min interval* and *max interval*, respectively.


Data layout
-----------

The raw metric data comprises a timestamp and a value, where the timestamp is a 64-bit signed integer denoting the POSIX timestamp in nanoseconds. The value is represented by an IEEE-754 floating point number with 64-bit precision.

An aggregate consists of an unsigned 64-bit integer for the count and an IEEE-754 floating point number with 64-bit precision for each of min, max, sum and integral, where the integral is defined as the sum weighted according to the time a data point was valid under the assumption of a last semantic. The aggregate also contains and signed 64-bit integer representing the duration for when actual data points back the aggregate. This active time is only of interest for intervals beginning before the first raw data point; otherwise, it is equivalent to the duration of the interval.


Storing a metric in an HTA
--------------------------

### Selecting good metric names

See [the Metric page in the MetricQ Wiki](https://github.com/metricq/metricq/wiki/Metrics#selecting-good-metric-names).

### Planning the aggregation parameters

A proper selection of the interval min, interval max, and interval factor parameters plays a big roll in the balance between the spacial overhead of the aggregation and the performance of the database.

Selecting the interval factor might be the most straightforward task. While the interval factor can be any number larger than `1`, it's recommended to simply use `10`, because of the more comfortable readable interval levels.

The interval min and max parameters, however, should be selected carefully with regards to the measurement sampling rate of the raw metric and the expected duration of the measurement. Both parameters are given as a duration in nanoseconds resolution and should be a multiple of the interval factor. Note that, due to downtimes of the measurement itself, the measurement sampling rate might differ from the average sampling rate.

For the interval min parameter, which defines the length of the intervals in the first aggregation level, we recommend a value equal to 30 times the length between two measurement samples. With this value, the first level comprises one-tenth the amount of storage space than the raw data level, because the aggregation data layout is about thrice as large as a single raw data point.

For long-running metrics with a sampling rate higher than once per second, we recommend as the interval max parameter the largest value of `interval_min * interval_factor ^ n`, which is smaller than one day. For metrics with a lower sampling rate, we recommend limiting the number of levels by selecting a smaller value for the interval max parameter.

With the recommended values for interval factor and interval min, the storage overhead for the aggregation of one metric is about 11 percent in comparison to the raw storage as a 128-bit timestamp-value pair list.

For example, given a raw metric with a sampling rate of 1000 Samples per second, we'd recommend an interval min of `3e7 ns` and thus an interval max of `3e14 ns`. These settings result in an approximate data rate of 17.7 kB/s or about 560GB per year.
