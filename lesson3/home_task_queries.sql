/*
 Завдання на SQL до лекції 02.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...
select 
	c."name",
	count(*)
from public.film_category fc
left join category c 
on fc.category_id = c.category_id 
group by 1
order by 2 desc
;

/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...
with top_movie as (
	select 
		i.film_id,
		count(*) as film_rent_num
	from public.rental r
	left join public.inventory i 
		on r.inventory_id = i.inventory_id
	group by i.film_id  
	)
select 
	concat(a.first_name, ' ', a.last_name),
	tm.film_rent_num
from top_movie tm
left join public.film_actor fa 
	on tm.film_id = fa.film_id
left join public.actor a 
	on fa.actor_id = a.actor_id
order by 2 desc
limit 10
;

/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
-- SQL code goes here...

-- Variant #1
with sales as (
	select 
		r.inventory_id,  
		sum(p.amount) as income 
	from public.rental r 
	left join public.payment p  
		on r.rental_id = p.rental_id  
	group by r.inventory_id 
	),
	category_sales as (
	select 
		fc.category_id,
		sum(s.income) as income
	from sales s
	left join public.inventory i 
		on s.inventory_id = i.inventory_id 
	left join public.film_category fc 
		on i.film_id = fc.film_id
	group by fc.category_id 
	)
select c."name" 
from category_sales cs
left join public.category c 
	on cs.category_id = c.category_id 
order by cs.income desc
limit 1
;

-- Variant #2
select 
	category
from public.sales_by_film_category sbfc 
order by total_sales desc
limit 1
;

/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
-- SQL code goes here...

select
	f.title 
from public.film f 
left join public.inventory i 
	on f.film_id = i.film_id 
where i.film_id is null
;

/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
-- SQL code goes here...
with cat_children as (
	select 
		fa.actor_id,
		count(*) as appear
	from public.film_actor fa 
	left join film_category fc 
		on fa.film_id = fc.film_id 
	left join public.category c 
		on fc.category_id = c.category_id 
	where c."name" = 'Children'
	group by fa.actor_id
	)
select 
	concat(a.first_name, ' ', a.last_name) as actor
from cat_children cc
left join public.actor a 
	on cc.actor_id = a.actor_id 
order by cc.appear desc
limit 3
;
     
/*
6.
Вивести міста з кількістю активних та неактивних клієнтів
(в активних customer.active = 1).
Результат відсортувати за кількістю неактивних клієнтів за спаданням.
*/
-- SQL code goes here...

with active_ones as (
	select 
		cl.city, 
		count(*) as num_active
	from public.customer_list cl
	left join public.customer c
		on cl.id = c.customer_id 
	where c.active = 1
	group by cl.city
	),
	inactive_ones as (
	select
		cl.city,
		count(*) as num_inactive
	from public.customer_list cl
	left join public.customer c
		on cl.id = c.customer_id 
	where c.active = 0
	group by cl.city
	)
select 
	coalesce(ao.city, io.city) as city,
	coalesce(num_active, 0) as num_active,
	coalesce(num_inactive, 0) as num_inactive
from active_ones ao
full outer join inactive_ones io
	on ao.city = io.city
order by num_inactive desc
;

