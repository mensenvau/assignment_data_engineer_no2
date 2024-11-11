## Task #1 (SQL Query Task)

### Problem Statement

Your task is to prepare a list of **cities** with:

1. The **date of the last reservation** made in each city.
2. The main **photo (photos[0])** of the most popular hotel (by number of bookings) in each city.

### Requirements:

-   Sort results in **ascending order by city name**.
-   If multiple hotels have the same booking count, order them by **hotel ID in ascending order**.

### Database Schema

You have the following tables:

#### `city` table

| Column | Type        | Description |
| ------ | ----------- | ----------- |
| id     | int         | Primary Key |
| name   | varchar(50) | City name   |

#### `hotel` table

| Column    | Type          | Description                    |
| --------- | ------------- | ------------------------------ |
| id        | int           | Primary Key                    |
| city_id   | int           | Foreign Key referencing `city` |
| name      | varchar(50)   | Hotel name                     |
| day_price | numeric(8, 2) | Price per day                  |
| photos    | jsonb         | Array of photo filenames       |

#### `booking` table

| Column       | Type | Description                     |
| ------------ | ---- | ------------------------------- |
| id           | int  | Primary Key                     |
| hotel_id     | int  | Foreign Key referencing `hotel` |
| booking_date | date | Date of booking                 |
| start_date   | date | Start date of the booking       |
| end_date     | date | End date of the booking         |

### Example Output

| city      | last_booking_date | hotel_id | hotel_photo |
| --------- | ----------------- | -------- | ----------- |
| Barcelona | 2019-04-06        | 3        | 3-1.jpg     |
| Roma      | 2019-04-06        | 6        | 6-1.jpg     |

### Notes

-   You need to find the most popular hotel based on the **number of bookings**.
-   Return the **primary photo** (photos[0]) of the most popular hotel.
-   Remember that the query will be run on different datasets, so make sure it is generic.

### Task #2
