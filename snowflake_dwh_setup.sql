-- Create dedicated database for car rental analytics
CREATE DATABASE car_rental;

-- Switch context to car_rental DB
USE car_rental;

-- Dimension: Locations (airports/cities)
CREATE OR REPLACE TABLE location_dim (
    location_key INTEGER AUTOINCREMENT PRIMARY KEY,
    location_id STRING UNIQUE NOT NULL,
    location_name STRING
);

-- Seed location records
INSERT INTO location_dim (location_id, location_name) VALUES
('LOC001', 'New York - JFK Airport'),
('LOC002', 'Los Angeles - LAX Airport'),
('LOC003', 'Chicago - OHare Airport'),
('LOC004', 'Houston - Bush Intercontinental Airport'),
('LOC005', 'San Francisco - SFO Airport'),
('LOC006', 'Miami - MIA Airport'),
('LOC007', 'Seattle - SeaTac Airport'),
('LOC008', 'Atlanta - Hartsfield-Jackson Airport'),
('LOC009', 'Dallas - DFW Airport'),
('LOC010', 'Denver - DEN Airport');

-- Dimension: Vehicles
CREATE OR REPLACE TABLE car_dim (
    car_key INTEGER AUTOINCREMENT PRIMARY KEY,
    car_id STRING UNIQUE NOT NULL,
    make STRING,
    model STRING,
    year INTEGER
);

-- Seed vehicle records
INSERT INTO car_dim (car_id, make, model, year) VALUES
('CAR001', 'Toyota', 'Camry', 2020),
('CAR002', 'Honda', 'Civic', 2019),
('CAR003', 'Ford', 'Mustang', 2021),
('CAR004', 'Chevrolet', 'Impala', 2018),
('CAR005', 'Tesla', 'Model S', 2022),
('CAR006', 'BMW', '3 Series', 2021),
('CAR007', 'Audi', 'A4', 2020),
('CAR008', 'Mercedes-Benz', 'C-Class', 2019),
('CAR009', 'Volkswagen', 'Passat', 2021),
('CAR010', 'Nissan', 'Altima', 2020);

-- Dimension: Calendar dates (used for foreign keys in fact)
CREATE OR REPLACE TABLE date_dim (
    date_key INTEGER PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    quarter INTEGER
);

-- Seed example dates (extend per need)
INSERT INTO date_dim (date_key, date, year, month, day, quarter) VALUES
(20250901, '2025-09-01', 2025, 9, 1, 3),
(20250902, '2025-09-02', 2025, 9, 2, 3),
(20250903, '2025-09-03', 2025, 9, 3, 3),
(20250904, '2025-09-04', 2025, 9, 4, 3),
(20250905, '2025-09-05', 2025, 9, 5, 3),
(20250906, '2025-09-06', 2025, 9, 6, 3),
(20250907, '2025-09-07', 2025, 9, 7, 3),
(20250908, '2025-09-08', 2025, 9, 8, 3),
(20250909, '2025-09-09', 2025, 9, 9, 3),
(20250910, '2025-09-10', 2025, 9, 10, 3),
(20250911, '2025-09-11', 2025, 9, 11, 3),
(20250912, '2025-09-12', 2025, 9, 12, 3),
(20250913, '2025-09-13', 2025, 9, 13, 3),
(20250914, '2025-09-14', 2025, 9, 14, 3),
(20250915, '2025-09-15', 2025, 9, 15, 3),
(20250916, '2025-09-16', 2025, 9, 16, 3),
(20250917, '2025-09-17', 2025, 9, 17, 3),
(20250918, '2025-09-18', 2025, 9, 18, 3),
(20250919, '2025-09-19', 2025, 9, 19, 3),
(20250920, '2025-09-20', 2025, 9, 20, 3),
(20250921, '2025-09-21', 2025, 9, 21, 3),
(20250922, '2025-09-22', 2025, 9, 22, 3),
(20250923, '2025-09-23', 2025, 9, 23, 3),
(20250924, '2025-09-24', 2025, 9, 24, 3),
(20250925, '2025-09-25', 2025, 9, 25, 3),
(20250926, '2025-09-26', 2025, 9, 26, 3),
(20250927, '2025-09-27', 2025, 9, 27, 3),
(20250928, '2025-09-28', 2025, 9, 28, 3),
(20250929, '2025-09-29', 2025, 9, 29, 3),
(20250930, '2025-09-30', 2025, 9, 30, 3);

-- Dimension: Customers with SCD2 columns
CREATE OR REPLACE TABLE customer_dim (
    customer_key INTEGER AUTOINCREMENT PRIMARY KEY,
    customer_id STRING UNIQUE NOT NULL,
    name STRING,
    email STRING,
    phone STRING,
    effective_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN
);


-- Fact: Rentals (FKs to all dims)
CREATE OR REPLACE TABLE rentals_fact (
    rental_id STRING PRIMARY KEY,
    customer_key INTEGER,
    car_key INTEGER,
    pickup_location_key INTEGER,
    dropoff_location_key INTEGER,
    start_date_key INTEGER,
    end_date_key INTEGER,
    amount FLOAT,
    quantity INTEGER,
    rental_duration_days INTEGER,
    total_rental_amount FLOAT,
    average_daily_rental_amount FLOAT,
    is_long_rental BOOLEAN,
    FOREIGN KEY (customer_key) REFERENCES customer_dim(customer_key),
    FOREIGN KEY (car_key) REFERENCES car_dim(car_key),
    FOREIGN KEY (pickup_location_key) REFERENCES location_dim(location_key),
    FOREIGN KEY (dropoff_location_key) REFERENCES location_dim(location_key),
    FOREIGN KEY (start_date_key) REFERENCES date_dim(date_key),
    FOREIGN KEY (end_date_key) REFERENCES date_dim(date_key)
);

-- File format for loading CSVs into customer_dim
CREATE FILE FORMAT csv_format TYPE=csv;

-- GCS storage integration and stage for customer daily files
CREATE STORAGE INTEGRATION gcs_car_rental_intg
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = GCS
STORAGE_ALLOWED_LOCATIONS = ('gcs://snowflake-projects-test-gds/car_rental_data/customer_daily_data/')
ENABLED = TRUE;

DESC INTEGRATION gcs_car_rental_intg;

-- External stage
CREATE OR REPLACE STAGE car_rental_data_stg
URL = 'gcs://snowflake-projects-test-gds/car_rental_data/customer_daily_data/'
STORAGE_INTEGRATION = gcs_car_rental_intg
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Quick checks
select * from customer_dim;

select * from rentals_fact;

-- truncate table rentals_fact;

-- truncate table customer_dim;
