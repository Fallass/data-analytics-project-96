SELECT COUNT(DISTINCT visitor_id) as "Пользователи", TO_CHAR(visit_date, 'YYYY-MM-DD') as "Дата"
from sessions
GROUP BY TO_CHAR(visit_date, 'YYYY-MM-DD')

select  medium as utm_medium, count(visitor_id) as visitors_count 
from sessions s 
group by utm_medium
order by  count(visitor_id) desc

select count(distinct lead_id) as lead, TO_CHAR(created_at, 'YYYY-MM-DD') as data
from leads l 
group by TO_CHAR(created_at, 'YYYY-MM-DD')

select COUNT(DISTINCT visitor_id)
from sessions s 

select count(distinct lead_id)
from leads l

select count(case when status_id = 142 then 1 else null end) as pokupka_count
from leads l

select 
    utm_source as s1, 
    sum(daily_spent) as total_daily, 
    to_char(campaign_date, 'YYYY-MM-DD') as campaign_date
from ya_ads
where utm_medium <> 'organic'
group by utm_source, campaign_date
order by campaign_date asc

select 

    utm_source as s2, 
    sum(daily_spent) as total_daily,
    to_char(campaign_date, 'YYYY-MM-DD') as campaign_date 
from vk_ads
group by utm_source, campaign_date
order by campaign_date asc
select  TO_CHAR(visit_date, 'YYYY-MM-DD'), lower(medium), count(visitor_id) as visitors_count
from sessions s 
group by 1,2
order by count(visitor_id) desc, lower(medium)

with LPC as (
    select 
        s.visitor_id,
        s.visit_date::date,
        s."source" as utm_source,
        s.medium as utm_medium,
        s.campaign as utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        row_number() over(partition by s.visitor_id order by s.visit_date desc) as rang
    from sessions s 
    left join leads l on s.visitor_id  = l.visitor_id 
        and s.visit_date <= l.created_at
    where s.medium in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
), unoin_ads as (
SELECT
        vk.campaign_date::date,
        vk.utm_source,
        vk.utm_medium,
        vk.utm_campaign,
        sum(vk.daily_spent) as daily_spent
    FROM vk_ads AS vk
    group by 1,2,3,4
    union all
    SELECT
        ya.campaign_date::date,
        ya.utm_source,
        ya.utm_medium,
        ya.utm_campaign,
        sum(ya.daily_spent) as daily_spent
    FROM ya_ads as ya 
    group by 1,2,3,4
)
select
        to_char(LPC.visit_date, 'Month') as start_month,
        LPC.utm_source,
        LPC.utm_medium,
        LPC.utm_campaign,
        count(LPC.visitor_id) as visitors_count
        from LPC
left join unoin_ads as u 
    on u.campaign_date = LPC.visit_date
    and u.utm_source = LPC.utm_source
    and    u.utm_medium = LPC.utm_medium 
    and    u.utm_campaign = LPC.utm_campaign 
where LPC.rang = 1
group by 1,2,3,4
order by visitors_count desc, LPC.utm_source asc, LPC.utm_medium asc, LPC.utm_campaign asc
with sessions_with_paid_mark as (
    select
        *,
        case when
                medium in (
                    'cpc',
                    'cpm',
                    'cpa',
                    'youtube',
                    'cpp',
                    'tg',
                    'social'
                )
                then 1
            else 0
        end as is_paid
    from sessions
),
visitors_with_leads as (
    select
        s.visitor_id,
        s.visit_date,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        s.medium as utm_medium,
        s.campaign as utm_campaign,
        lower(s.source) as utm_source,
        row_number() over (
            partition by s.visitor_id
            order by s.is_paid desc, s.visit_date desc
        ) as rn
    from sessions_with_paid_mark as s
    left join leads as l
        on
            s.visitor_id = l.visitor_id
            and s.visit_date <= l.created_at
    where s.is_paid = 1
),
attribution as (
    select *
    from visitors_with_leads
    where rn = 1
),
aggregated_data as (
    select
        utm_source,
        utm_medium,
        utm_campaign,
        date(visit_date) as visit_date,
        count(visitor_id) as visitors_count,
        count(
            case
                when created_at is not null then visitor_id
            end
        ) as leads_count,
        count(case when status_id = 142 then visitor_id end) as purchases_count,
        sum(case when status_id = 142 then amount end) as revenue
    from attribution
    group by 1, 2, 3, 4
),
marketing_data as (
    select
        date(campaign_date) as visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        sum(daily_spent) as total_cost
    from ya_ads
    group by 1, 2, 3, 4
    union all
    select
        date(campaign_date) as visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        sum(daily_spent) as total_cost
    from vk_ads
    group by 1, 2, 3, 4
)
select
    a.visit_date,
    a.visitors_count,
    a.utm_source,
    a.utm_medium,
    a.utm_campaign,
    m.total_cost,
    a.leads_count,
    a.purchases_count,
    a.revenue
from aggregated_data as a
left join marketing_data as m
    on
        a.visit_date = m.visit_date
        and lower(a.utm_source) = m.utm_source
        and lower(a.utm_medium) = m.utm_medium
        and lower(a.utm_campaign) = m.utm_campaign
order by 9 desc nulls last, 1, 2 desc, 3, 4
limit 15