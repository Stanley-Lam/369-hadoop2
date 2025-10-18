# 369-hadoop lab 3

Task 1: CountryRequestCount

LogMapper parses each line of access log and emits (hostname, LOG 1).
CountryMapper parses csv file and emits (hostname, country).
JoinReducer takes all values and performs a reduce-side join.
It aggregates number of log entries per hostname and associate them with corresponding country.
It emits (country, total).
CountryCountMapper just inverts the output (count, country).
DescendingIntComparator reverses natural sort order to sort count.
Reducer emits (country, count)

Task 2: CountryRequestCount

LogMapper parses each line of access log and emits (hostname, LOG 1).
CountryMapper parses csv file and emits (hostname, country).
JoinReducer takes all values and performs a reduce-side join.
IdentityMapper reads intermediate output and emits (country, count).
SumReducer aggregates all counts for each (country, url) pair.
Reducer emits (country, total)

Task 3: CountryUrlReport
LogMapper parses each line of access log and emits (hostname, LOG 1).
CountryMapper parses csv file and emits (hostname, country).
JoinReducer takes all values and performs a reduce-side join.
Reducer emits (url, country)
UrlCountryMapper reads intermediate output and emits (url, country)
CountryListReducer aggregates all countries per url into treeset to ensure uniqueness and alphabetical order.
Reducer emits (url, country_list)
