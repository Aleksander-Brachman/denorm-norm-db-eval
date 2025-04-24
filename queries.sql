--SELECT 1
-- denorm
SELECT * FROM car_sale_info;
-- norm
SELECT c.vin, c.year, c.make, c.model, b.body,
t.transmission, cl.color, c.car_condition, c.odometer, sa.mmr, 
sa.selling_price, sa.sale_date, se.seller, se.state 
FROM car_details c 
JOIN body_info b ON c.body_id = b.body_id 
JOIN transmission_info t ON c.transmission_id = t.transmission_id 
JOIN color_info cl ON c.color_id = cl.color_id 
JOIN sale_details sa ON c.vin = sa.vin 
JOIN seller_info se ON sa.seller_id = se.seller_id;

--SELECT 2
-- denorm
SELECT make, round(avg(car_condition),2) AS 'avg_car_condition', 
round(avg(mmr),2) AS 'avg_mmr' 
FROM car_sale_info 
GROUP BY make 
ORDER BY avg(mmr) DESC;
-- norm
SELECT c.make AS 'make', round(avg(c.car_condition),2) AS 'avg_car_condition', 
round(avg(sa.mmr),2) AS 'avg_mmr' 
FROM car_details c 
JOIN sale_details sa ON c.vin = sa.vin 
GROUP BY c.make 
ORDER BY avg(sa.mmr) DESC;

--SELECT 3
-- denorm
SELECT state, round(avg(selling_price),2) as "avg_selling_price" 
FROM car_sale_info 
GROUP BY state 
ORDER BY avg(selling_price) DESC;
-- norm
SELECT se.state AS "state", round(avg(sa.selling_price),2) AS "avg_selling_price" 
FROM seller_info se 
JOIN sale_details sa ON se.seller_id = sa.seller_id 
GROUP BY se.state 
ORDER BY avg(sa.selling_price) DESC;

-- UPDATE 1
-- denorm
UPDATE car_sale_info SET selling_price = 1.05 * selling_price;
--norm
UPDATE sale_details SET selling_price = 1.05 * selling_price;

-- UPDATE 2
-- denorm
UPDATE car_sale_info SET seller = 'max & leo venters motors inc' 
WHERE seller = 'leo venters motors inc' AND state = 'nc';
-- norm
UPDATE seller_info SET seller='leo venters motors inc'
WHERE seller_id=5793;

-- DELETE 1
-- denorm
DELETE FROM car_sale_info LIMIT 10000; -- wersja dla MySQL/MariaDB
DELETE FROM car_sale_info 
WHERE vin IN (
    SELECT vin FROM car_sale_info LIMIT 10000
    ); -- wersja dla PostgreSQL

-- norm
CREATE TEMPORARY TABLE temp_vin AS SELECT vin FROM car_details LIMIT 10000;
DELETE sa FROM sale_details sa JOIN temp_vin t ON sa.vin = t.vin;
DELETE c FROM car_details c JOIN temp_vin t ON c.vin = t.vin;
DROP TEMPORARY TABLE temp_vin; -- wersja dla MySQL/MariaDB

DELETE FROM car_details WHERE vin IN (SELECT vin FROM car_details LIMIT 10000); 
DELETE FROM sale_details WHERE vin IN (SELECT vin FROM car_details LIMIT 10000); -- wersja dla PostgreSQL
