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

CREATE TABLE time(
    time_id INT IDENTITY(1,1) PRIMARY KEY,
    hour INT NOT NULL,
    minute INT NOT NULL,
    time_str VARCHAR(5) NOT NULL
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
    weather_id INT NOT NULL FOREIGN KEY REFERENCES weather_info(weather_id),
    date_id INT NOT NULL FOREIGN KEY REFERENCES date(date_id),
    time_id INT NOT NULL FOREIGN KEY REFERENCES time(time_id),
    temperature FLOAT,
    feels_like FLOAT,
    temp_min FLOAT,
    temp_max FLOAT,
    pressure INT,
    sea_level INT ,
    ground_level INT ,
    humidity INT,
    visibility INT,

    wind_speed FLOAT,
    wind_deg INT,
    wind_gust FLOAT ,

    cloudiness INT,
    rain_1h FLOAT NULL,
    snow_1h FLOAT NULL
);

CREATE INDEX idx_weather_city ON weather(city_id);
CREATE INDEX idx_weather_date ON weather(date_id);
CREATE INDEX idx_weather_time ON weather(time_id);