CREATE TABLE IF NOT EXISTS `CS306Project`.`locationdataset` (
  `Country` TEXT NULL DEFAULT NULL,
  `Year` INT NULL DEFAULT NULL,
  `Code` TEXT NOT NULL,
  `Population` INT NULL DEFAULT NULL,
  `GDP` BIGINT NULL DEFAULT NULL,
  PRIMARY KEY (`Code`));

CREATE TABLE IF NOT EXISTS `CS306Project`.`sharedeathsairpollution` (
  `Country` TEXT NULL DEFAULT NULL,
  `Code` TEXT NOT NULL,
  `Year` INT NULL DEFAULT NULL,
  `Deaths  AirP(%)` DOUBLE NULL DEFAULT NULL,
  `locationdataset_Code` TEXT NOT NULL,
  PRIMARY KEY (`Code`, `locationdataset_Code`),
  INDEX `fk_sharedeathsairpollution_locationdataset1_idx` (`locationdataset_Code` ASC) VISIBLE,
  CONSTRAINT `fk_sharedeathsairpollution_locationdataset1`
    FOREIGN KEY (`locationdataset_Code`)
    REFERENCES `CS306Project`.`locationdataset` (`Code`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);


CREATE TABLE IF NOT EXISTS `CS306Project`.`windcleaned` (
  `Country` TEXT NULL DEFAULT NULL,
  `Code` TEXT NOT NULL,
  `Year` INT NULL DEFAULT NULL,
  `Wind per capita (kWh - equivalent)` DOUBLE NULL DEFAULT NULL,
  `locationdataset_Code` TEXT NOT NULL,
  PRIMARY KEY (`Code`, `locationdataset_Code`),
  INDEX `fk_windcleaned_locationdataset1_idx` (`locationdataset_Code` ASC) VISIBLE,
  CONSTRAINT `fk_windcleaned_locationdataset1`
    FOREIGN KEY (`locationdataset_Code`)
    REFERENCES `CS306Project`.`locationdataset` (`Code`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);
    
    CREATE TABLE IF NOT EXISTS `CS306Project`.`co2emission` (
  `Country` TEXT NULL DEFAULT NULL,
  `Code` TEXT NOT NULL,
  `Year` INT NULL DEFAULT NULL,
  `Annual CO2 emissions` INT NULL DEFAULT NULL,
  `locationdataset_Code` TEXT NOT NULL,
  PRIMARY KEY (`Code`, `locationdataset_Code`),
  INDEX `fk_co2emission_locationdataset_idx` (`locationdataset_Code` ASC) VISIBLE,
  CONSTRAINT `fk_co2emission_locationdataset`
    FOREIGN KEY (`locationdataset_Code`)
    REFERENCES `CS306Project`.`locationdataset` (`Code`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);
    
    
    CREATE TABLE IF NOT EXISTS `CS306Project`.`fossilfuelconsumptionfinal` (
  `Country` TEXT NULL DEFAULT NULL,
  `Code` TEXT NOT NULL,
  `Year` INT NULL DEFAULT NULL,
  `Fossil fuels (TWh)` INT NULL DEFAULT NULL,
  `locationdataset_Code` TEXT NOT NULL,
  PRIMARY KEY (`Code`, `locationdataset_Code`),
  INDEX `fk_fossilfuelconsumptionfinal_locationdataset1_idx` (`locationdataset_Code` ASC) VISIBLE,
  CONSTRAINT `fk_fossilfuelconsumptionfinal_locationdataset1`
    FOREIGN KEY (`locationdataset_Code`)
    REFERENCES `CS306Project`.`locationdataset` (`Code`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);
    
    
    CREATE TABLE IF NOT EXISTS `CS306Project`.`ozonecleaned` (
  `Country` TEXT NULL DEFAULT NULL,
  `Code` TEXT NOT NULL,
  `Year` INT NULL DEFAULT NULL,
  `Ozone concentration-StateofGlobalAir` INT NULL DEFAULT NULL,
  `locationdataset_Code` TEXT NOT NULL,
  PRIMARY KEY (`Code`, `locationdataset_Code`),
  INDEX `fk_ozonecleaned_locationdataset1_idx` (`locationdataset_Code` ASC) VISIBLE,
  CONSTRAINT `fk_ozonecleaned_locationdataset1`
    FOREIGN KEY (`locationdataset_Code`)
    REFERENCES `CS306Project`.`locationdataset` (`Code`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);
    
    