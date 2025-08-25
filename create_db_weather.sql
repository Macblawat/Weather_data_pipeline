CREATE TABLE city (
    city_id INT IDENTITY(1,1) PRIMARY KEY,
    api_city_id INT NOT NULL UNIQUE,
    city_name NVARCHAR(100) NOT NULL,
    country_id NVARCHAR(10) NOT NULL,
    latitude FLOAT,
    longitude FLOAT
);

CREATE TABLE date (
    date_id INT IDENTITY(1,1) PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    weekday NVARCHAR(15)
);

CREATE TABLE weather_info (
    weather_id INT IDENTITY(1,1) PRIMARY KEY,
    api_weather_id INT NOT NULL UNIQUE,  
    main NVARCHAR(50),  
    description NVARCHAR(255)
);

CREATE TABLE  weather (
    fact_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    city_id INT NOT NULL FOREIGN KEY REFERENCES city(city_id),
    weather_id INT NULL FOREIGN KEY REFERENCES weather_info(weather_id),
    date_id INT NOT NULL FOREIGN KEY REFERENCES date(date_id),

    timestamp_utc DATETIME NOT NULL, 
    temperature FLOAT,
    feels_like FLOAT,
    temp_min FLOAT,
    temp_max FLOAT,
    pressure INT,
    sea_level INT NULL,
    ground_level INT NULL,
    humidity INT,
    visibility INT,

    wind_speed FLOAT,
    wind_deg INT,
    wind_gust FLOAT NULL,

    cloudiness INT,
    rain_1h FLOAT NULL,
    snow_1h FLOAT NULL,

);