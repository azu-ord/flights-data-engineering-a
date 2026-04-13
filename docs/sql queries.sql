#  sql queries


/*
SELECT origin_airport, destination_airport,
	COUNT(*) as numero_vuelos_por_ruta
from flights
group by origin_airport, destination_airport
order by numero_vuelos_por_ruta desc
LIMIT 10;
*/

/*
SELECT
    airline,
    COUNT(*) AS total_flights,
    SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS cancelled_flights,
    SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS avg_cancellations
FROM flights
GROUP BY airline
order by avg_cancellations desc
limit 10;

*/

/*
select 
	cancellation_reason,
	COUNT(*) as count_cancellation_number
from FLIGHTS
where cancellation_reason is not null
group by cancellation_reason
order by count_cancellation_number desc;
*/



/* P4*/

/*
select
	sum(departure_delay) as sum_departure_delay ,
	AVG(departure_delay) as avg_departure_delay,
	COUNT(*) as count_delay_per_month
from flights
WHERE cancelled = 0 AND departure_delay > 0
group by month;
*/


/*P5*/
/*
select
	origin_airport,
	sum(weather_delay) as sum_weather_delay
from flights 
where weather_delay is not null
group by origin_airport
order by sum_weather_delay desc
limit 10;
*/


/* W1*/

/*
select *
from (
select
	airline,
	flight_number,
	arrival_delay,
	origin_airport,
	destination_airport,
	RANK() over(
	partition by airline
	order by arrival_delay  desc nulls LAST
	) as most_delay_flight_per_airline
from flights
)
where most_delay_flight_per_airline =1
order by arrival_delay desc;

*/

/* W2 */
/*
with vuelos_lax as (
select 
	flight_number,
	airline,
	year,
	month,
	day,
	origin_airport,
	destination_airport,
	scheduled_departure,
	ROW_NUMBER() over(
	partition by year,month,day,origin_airport
	order by scheduled_departure asc
	) AS rn
from flights 
where origin_airport = 'LAX' and year = '2015' and "month" = 1 and "day" = 1
)
select 
	rn,
	flight_number,
    airline,
    destination_airport,
    scheduled_departure
from vuelos_lax 
where rn <=5
order by rn
	*/

*/*P5*/


WITH vuelos_por_mes AS (
    SELECT
        month,
        COUNT(*) AS total_vuelos
    FROM flights
    WHERE year = 2015
    GROUP BY month
)
SELECT
    month,
    total_vuelos,
    LAG(total_vuelos) OVER (ORDER BY month) AS vuelos_mes_anterior,
    total_vuelos - LAG(total_vuelos) OVER (ORDER BY month) AS diferencia_absoluta,
    (
        (total_vuelos - LAG(total_vuelos) OVER (ORDER BY month)) * 100.0
        / LAG(total_vuelos) OVER (ORDER BY month)
    ) AS cambio_porcentual
FROM vuelos_por_mes
ORDER BY month;











