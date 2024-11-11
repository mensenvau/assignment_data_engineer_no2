/*
Please, write your SQL query below.
*/

with cte as (
    select 
        h.city_id,
        h.id as hotel_id,
        h.photos->>0 as hotel_photo,
        count(b.id) as booking_count,
        row_number() over(partition by h.city_id order by count(b.id) desc, h.id desc) as rn
    from hotel h
    left join booking b on h.id = b.hotel_id
    group by h.id, h.city_id
), cte_max as (
    select 
        h.city_id,
        max(b.booking_date) as booking_date
    from hotel h
    left join booking b on h.id = b.hotel_id
    group by h.city_id
)

select 
    c.name,
    cte_max.booking_date,
    cte.hotel_id,
    cte.hotel_photo
from city  as c
left join cte on c.id = cte.city_id
left join cte_max on c.id = cte_max.city_id
where rn = 1
order by c.name
