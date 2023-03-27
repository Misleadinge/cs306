SET GLOBAL general_log = 'ON'

CREATE TABLE IF NOT EXISTS `CS306SchemeForLog`.`locationdatasetnew` (
  `Country` TEXT NULL DEFAULT NULL,
  `Year` INT NULL DEFAULT NULL,
  `Code` TEXT NOT NULL,
  `Population` INT NULL DEFAULT NULL,
  PRIMARY KEY (`Code`));

CREATE TABLE IF NOT EXISTS `CS306SchemeForLog`.`sharedeathsairpollution` (
  `Country` TEXT NULL DEFAULT NULL,
  `Code` TEXT NOT NULL,
  `Year` INT NULL DEFAULT NULL,
  `Deaths  AirP(%)` DOUBLE NULL DEFAULT NULL,
  `locationdatasetnew_Code` TEXT NOT NULL,
  PRIMARY KEY (`Code`, `locationdatasetnew_Code`),
  INDEX `fk_sharedeathsairpollution_locationdatasetnew1_idx` (`locationdatasetnew_Code` ASC) VISIBLE,
  CONSTRAINT `fk_sharedeathsairpollution_locationdatasetnew1`
    FOREIGN KEY (`locationdatasetnew_Code`)
    REFERENCES `CS306Project`.`locationdatasetnew` (`Code`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);


CREATE TABLE IF NOT EXISTS `CS306SchemeForLog`.`windcleaned` (
  `Country` TEXT NULL DEFAULT NULL,
  `Code` TEXT NOT NULL,
  `Year` INT NULL DEFAULT NULL,
  `Wind per capita (kWh - equivalent)` DOUBLE NULL DEFAULT NULL,
  `locationdatasetnew_Code` TEXT NOT NULL,
  PRIMARY KEY (`Code`, `locationdatasetnew_Code`),
  INDEX `fk_windcleaned_locationdatasetnew1_idx` (`locationdatasetnew_Code` ASC) VISIBLE,
  CONSTRAINT `fk_windcleaned_locationdatasetnew1`
    FOREIGN KEY (`locationdatasetnew_Code`)
    REFERENCES `CS306Project`.`locationdatasetnew` (`Code`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);
    
    CREATE TABLE IF NOT EXISTS `CS306SchemeForLog`.`co2emission` (
  `Country` TEXT NULL DEFAULT NULL,
  `Code` TEXT NOT NULL,
  `Year` INT NULL DEFAULT NULL,
  `Annual CO2 emissions` INT NULL DEFAULT NULL,
  `locationdatasetnew_Code` TEXT NOT NULL,
  PRIMARY KEY (`Code`, `locationdatasetnew_Code`),
  INDEX `fk_co2emission_locationdatasetnew_idx` (`locationdatasetnew_Code` ASC) VISIBLE,
  CONSTRAINT `fk_co2emission_locationdatasetnew`
    FOREIGN KEY (`locationdatasetnew_Code`)
    REFERENCES `CS306Project`.`locationdatasetnew` (`Code`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);
    
    
    CREATE TABLE IF NOT EXISTS `CS306SchemeForLog`.`fossilfuelconsumptionfinal` (
  `Country` TEXT NULL DEFAULT NULL,
  `Code` TEXT NOT NULL,
  `Year` INT NULL DEFAULT NULL,
  `Fossil fuels (TWh)` INT NULL DEFAULT NULL,
  `locationdatasetnew_Code` TEXT NOT NULL,
  PRIMARY KEY (`Code`, `locationdatasetnew_Code`),
  INDEX `fk_fossilfuelconsumptionfinal_locationdatasetnew1_idx` (`locationdatasetnew_Code` ASC) VISIBLE,
  CONSTRAINT `fk_fossilfuelconsumptionfinal_locationdatasetnew1`
    FOREIGN KEY (`locationdatasetnew_Code`)
    REFERENCES `CS306Project`.`locationdatasetnew` (`Code`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);
    
    
    CREATE TABLE IF NOT EXISTS `CS306SchemeForLog`.`ozonecleaned` (
  `Country` TEXT NULL DEFAULT NULL,
  `Code` TEXT NOT NULL,
  `Year` INT NULL DEFAULT NULL,
  `Ozone concentration-StateofGlobalAir` INT NULL DEFAULT NULL,
  `locationdatasetnew_Code` TEXT NOT NULL,
  PRIMARY KEY (`Code`, `locationdatasetnew_Code`),
  INDEX `fk_ozonecleaned_locationdatasetnew1_idx` (`locationdatasetnew_Code` ASC) VISIBLE,
  CONSTRAINT `fk_ozonecleaned_locationdatasetnew1`
    FOREIGN KEY (`locationdatasetnew_Code`)
    REFERENCES `CS306Project`.`locationdatasetnew` (`Code`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);
    