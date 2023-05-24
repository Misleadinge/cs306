SET GLOBAL general_log = 'ON';

CREATE TABLE `locationdataset` (
  `Country` text,
  `Year` int NOT NULL,
  `Code` varchar(50) NOT NULL,
  `Population` double DEFAULT NULL,
  `GDP` bigint DEFAULT NULL,
  PRIMARY KEY (`Year`,`Code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `co2emission` (
  `Entity` text,
  `Code` varchar(50) NOT NULL,
  `Year` bigint NOT NULL,
  `Annual CO2 emissions` bigint DEFAULT NULL,
  PRIMARY KEY (`Code`,`Year`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `fossilfuelconsumptionfinal` (
  `Entity` text,
  `Code` varchar(50) NOT NULL,
  `Year` bigint NOT NULL,
  `Fossil fuels (TWh)` bigint DEFAULT NULL,
  PRIMARY KEY (`Code`,`Year`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `ozonecleaned` (
  `Entity` text,
  `Code` varchar(50) NOT NULL,
  `Year` int NOT NULL,
  `Ozone concentration-StateofGlobalAir` int DEFAULT NULL,
  PRIMARY KEY (`Code`,`Year`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `sharedeathsairpollution` (
  `Entity` text,
  `Code` varchar(50) NOT NULL,
  `Year` int NOT NULL,
  `Deaths  AirP(%)` double DEFAULT NULL,
  PRIMARY KEY (`Code`,`Year`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `windcleaned` (
  `Entity` text,
  `Code` varchar(50) NOT NULL,
  `Year` int NOT NULL,
  `Wind per capita (kWh - equivalent)` double DEFAULT NULL,
  PRIMARY KEY (`Code`,`Year`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE VIEW `high_gdp_countries` AS
SELECT `Code`, `Country`, `Year`, `GDP`
FROM `locationdataset`
WHERE `GDP` > (SELECT AVG(`GDP`) FROM `locationdataset` WHERE `GDP` IS NOT NULL);

CREATE VIEW `low_death_rate_countries` AS
SELECT `Code`, `Country`, `Year`, `Deaths  AirP(%)`
FROM `sharedeathsairpollution`
WHERE `Deaths  AirP(%)` < (SELECT AVG(`Deaths  AirP(%)`) FROM `sharedeathsairpollution` WHERE `Deaths  AirP(%)` IS NOT NULL);

CREATE 
    ALGORITHM = UNDEFINED 
    DEFINER = `root`@`localhost` 
    SQL SECURITY DEFINER
VIEW `emissions_and_ozone` AS
    SELECT 
        `l`.`Country` AS `Country`,
        `l`.`Year` AS `Year`,
        `l`.`Code` AS `Code`,
        `c`.`Annual CO2 emissions` AS `Annual CO2 emissions`,
        `f`.`Fossil fuels (TWh)` AS `Fossil fuels (TWh)`,
        `o`.`Ozone concentration-StateofGlobalAir` AS `Ozone concentration-StateofGlobalAir`
    FROM
        (((`locationdataset` `l`
        JOIN `co2emission` `c` ON (((`l`.`Year` = `c`.`Year`)
            AND (`l`.`Code` = `c`.`Code`))))
        JOIN `fossilfuelconsumptionfinal` `f` ON (((`l`.`Year` = `f`.`Year`)
            AND (`l`.`Code` = `f`.`Code`))))
        JOIN `ozonecleaned` `o` ON (((`l`.`Year` = `o`.`Year`)
            AND (`l`.`Code` = `o`.`Code`))));
            
            
CREATE 
    ALGORITHM = UNDEFINED 
    DEFINER = `root`@`localhost` 
    SQL SECURITY DEFINER
VIEW `gdp_and_emissions` AS
    SELECT 
        `l`.`Country` AS `Country`,
        `l`.`Year` AS `Year`,
        `l`.`Code` AS `Code`,
        `l`.`GDP` AS `GDP`,
        `c`.`Annual CO2 emissions` AS `Annual CO2 emissions`,
        `f`.`Fossil fuels (TWh)` AS `Fossil fuels (TWh)`
    FROM
        ((`locationdataset` `l`
        JOIN `co2emission` `c` ON (((`l`.`Year` = `c`.`Year`)
            AND (`l`.`Code` = `c`.`Code`))))
        JOIN `fossilfuelconsumptionfinal` `f` ON (((`l`.`Year` = `f`.`Year`)
            AND (`l`.`Code` = `f`.`Code`))));
            

CREATE 
    ALGORITHM = UNDEFINED 
    DEFINER = `root`@`localhost` 
    SQL SECURITY DEFINER
VIEW `gdp_and_pollution` AS
    SELECT 
        `l`.`Country` AS `Country`,
        `l`.`Year` AS `Year`,
        `l`.`Code` AS `Code`,
        `l`.`GDP` AS `GDP`,
        `o`.`Ozone concentration-StateofGlobalAir` AS `Ozone concentration-StateofGlobalAir`,
        `s`.`Deaths  AirP(%)` AS `Deaths  AirP(%)`
    FROM
        ((`locationdataset` `l`
        JOIN `ozonecleaned` `o` ON (((`l`.`Year` = `o`.`Year`)
            AND (`l`.`Code` = `o`.`Code`))))
        JOIN `sharedeathsairpollution` `s` ON (((`l`.`Year` = `s`.`Year`)
            AND (`l`.`Code` = `s`.`Code`))));
            
CREATE 
    ALGORITHM = UNDEFINED 
    DEFINER = `root`@`localhost` 
    SQL SECURITY DEFINER
VIEW `population_and_emissions` AS
    SELECT 
        `l`.`Country` AS `Country`,
        `l`.`Year` AS `Year`,
        `l`.`Code` AS `Code`,
        `l`.`Population` AS `Population`,
        `c`.`Annual CO2 emissions` AS `Annual CO2 emissions`,
        `w`.`Wind per capita (kWh - equivalent)` AS `Wind per capita (kWh - equivalent)`
    FROM
        ((`locationdataset` `l`
        JOIN `co2emission` `c` ON (((`l`.`Year` = `c`.`Year`)
            AND (`l`.`Code` = `c`.`Code`))))
        JOIN `windcleaned` `w` ON (((`l`.`Year` = `w`.`Year`)
            AND (`l`.`Code` = `w`.`Code`))));
            
CREATE 
    ALGORITHM = UNDEFINED 
    DEFINER = `root`@`localhost` 
    SQL SECURITY DEFINER
VIEW `wind_and_airpollution` AS
    SELECT 
        `l`.`Country` AS `Country`,
        `l`.`Year` AS `Year`,
        `l`.`Code` AS `Code`,
        `w`.`Wind per capita (kWh - equivalent)` AS `Wind per capita (kWh - equivalent)`,
        `s`.`Deaths  AirP(%)` AS `Deaths  AirP(%)`
    FROM
        ((`locationdataset` `l`
        JOIN `windcleaned` `w` ON (((`l`.`Year` = `w`.`Year`)
            AND (`l`.`Code` = `w`.`Code`))))
        JOIN `sharedeathsairpollution` `s` ON (((`l`.`Year` = `s`.`Year`)
            AND (`l`.`Code` = `s`.`Code`))));
            
            

SELECT h.*
FROM `high_gdp_countries` h
LEFT JOIN `low_death_rate_countries` l ON h.`Code` = l.`Code` AND h.`Year` = l.`Year`
WHERE l.`Code` IS NULL;

SELECT COUNT(*)
FROM (
  SELECT h.*
  FROM `high_gdp_countries` h
  LEFT JOIN `low_death_rate_countries` l ON h.`Code` = l.`Code` AND h.`Year` = l.`Year`
  WHERE l.`Code` IS NULL
) AS difference_set;

SELECT l.`Country`, l.`Year`, l.`GDP`
FROM `locationdataset` l
WHERE l.`Code` IN (
  SELECT `Code`
  FROM `co2emission`
  WHERE `Annual CO2 emissions` > (SELECT AVG(`Annual CO2 emissions`) FROM `co2emission` WHERE `Annual CO2 emissions` IS NOT NULL)
);

SELECT l.`Country`, l.`Year`, l.`GDP`
FROM `locationdataset` l
WHERE EXISTS (
  SELECT 1
  FROM `co2emission` c
  WHERE c.`Code` = l.`Code` AND c.`Annual CO2 emissions` > (SELECT AVG(`Annual CO2 emissions`) FROM `co2emission` WHERE `Annual CO2 emissions` IS NOT NULL)
);

-- For the "IN" operator
SELECT COUNT(*)
FROM (
  SELECT l.`Country`, l.`Year`, l.`GDP`
  FROM `locationdataset` l
  WHERE l.`Code` IN (
    SELECT `Code`
    FROM `co2emission`
    WHERE `Annual CO2 emissions` > (SELECT AVG(`Annual CO2 emissions`) FROM `co2emission` WHERE `Annual CO2 emissions` IS NOT NULL)
  )
) AS in_result;

-- For the "EXISTS" operator
SELECT COUNT(*)
FROM (
  SELECT l.`Country`, l.`Year`, l.`GDP`
  FROM `locationdataset` l
  WHERE EXISTS (
    SELECT 1
    FROM `co2emission` c
    WHERE c.`Code` = l.`Code` AND c.`Annual CO2 emissions` > (SELECT AVG(`Annual CO2 emissions`) FROM `co2emission` WHERE `Annual CO2 emissions` IS NOT NULL)
  )
) AS exists_result;


SELECT c.`Year`, SUM(c.`Annual CO2 emissions`) AS Total_CO2_Emissions, AVG(l.`GDP`) AS Average_GDP
FROM `co2emission` c
JOIN `locationdataset` l ON c.`Code` = l.`Code` AND c.`Year` = l.`Year`
GROUP BY c.`Year`;


SELECT MIN(`Year`) AS Min_Year, MAX(`Year`) AS Max_Year, COUNT(`Year`) AS Num_Countries
FROM (
  SELECT o.`Year`
  FROM `ozonecleaned` o
  WHERE o.`Ozone concentration-StateofGlobalAir` > 50
  GROUP BY o.`Year`
  HAVING COUNT(o.`Code`) > 1
) AS subquery;


SELECT w.`Year`, SUM(w.`Wind per capita (kWh - equivalent)`) AS Total_Wind_Power
FROM `windcleaned` w
JOIN `sharedeathsairpollution` s ON w.`Code` = s.`Code` AND w.`Year` = s.`Year`
WHERE s.`Deaths  AirP(%)` > 5
GROUP BY w.`Year`;

SELECT MIN(`Annual CO2 emissions`) AS Min_CO2_Emissions, MAX(`Annual CO2 emissions`) AS Max_CO2_Emissions
FROM `co2emission`;

ALTER TABLE `co2emission`
ADD CONSTRAINT `chk_annual_co2_emissions_range` CHECK (`Annual CO2 emissions` >= min_value AND `Annual CO2 emissions` <= max_value);


INSERT INTO `co2emission` (`Entity`, `Code`, `Year`, `Annual CO2 emissions`)
VALUES ('Some Entity', 'some_code', some_year, invalid_value);

DELIMITER //

CREATE TRIGGER `co2emission_before_insert`
BEFORE INSERT ON `co2emission`
FOR EACH ROW
BEGIN
  IF NEW.`Annual CO2 emissions` < min_value THEN
    SET NEW.`Annual CO2 emissions` = min_value;
  ELSEIF NEW.`Annual CO2 emissions` > max_value THEN
    SET NEW.`Annual CO2 emissions` = max_value;
  END IF;
END;
//

CREATE TRIGGER `co2emission_before_update`
BEFORE UPDATE ON `co2emission`
FOR EACH ROW
BEGIN
  IF NEW.`Annual CO2 emissions` < min_value THEN
    SET NEW.`Annual CO2 emissions` = min_value;
  ELSEIF NEW.`Annual CO2 emissions` > max_value THEN
    SET NEW.`Annual CO2 emissions` = max_value;
  END IF;
END;
//

DELIMITER ;

DELIMITER //

CREATE PROCEDURE `GetCovid19DataByCountry` (IN iso_code VARCHAR(50))
BEGIN
  SELECT `date`, `cases`, `deaths`
  FROM `covid19_data`
  WHERE `iso_code` = iso_code
  ORDER BY `date` DESC;
END;
//

DELIMITER ;

CALL `GetCovid19DataByCountry`('US');
CALL `GetCovid19DataByCountry`('GB');


