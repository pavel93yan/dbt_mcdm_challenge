{{ config(materialized="view", tags=["daily"]) }}

with
    pre_proc as (
        select
            cast(__insert_date as datetime) as __insert_date,
            cast(ad_id as string) as ad_id,
            coalesce(cast(add_to_cart as int64), 0) as add_to_cart,
            cast(adset_id as string) as adset_id,
            cast(campaign_id as string) as campaign_id,
            cast(channel as string) as channel,
            coalesce(cast(clicks as int64), 0) as clicks,
            coalesce(cast(impressions as int64), 0) as impressions,
            coalesce(cast(spend as int64), 0) as spend,
            coalesce(cast(comments as int64), 0) as comments,
            cast(creative_id as string) as creative_id,
            cast(nullif(trim(creative_title), '') as string) as creative_title,
            cast(nullif(trim(objective), '') as string) as objective,
            cast(nullif(trim(buying_type), '') as string) as buying_type,
            cast(nullif(trim(campaign_type), '') as string) as campaign_type,
            cast(nullif(trim(creative_body), '') as string) as creative_body,
            cast(`date` as date) as `date`,
            coalesce(cast(likes as int64), 0) as likes,
            coalesce(cast(shares as int64), 0) as shares,
            coalesce(cast(views as int64), 0) as video_views,
            coalesce(cast(mobile_app_install as int64), 0) as installs,
            coalesce(cast(inline_link_clicks as int64), 0) as link_clicks,
            coalesce(cast(purchase as int64), 0) as purchase,
            coalesce(cast(complete_registration as int64), 0) as registrations,
            coalesce(cast(purchase_value as int64), 0) as purchase_value,
            coalesce(cast((comments + clicks + shares + purchase + views) as int64), 0) as engagements,
            coalesce(cast(purchase as int64), 0) as total_conversions,
            
            -- down below fields that are not in data source 
            cast(null as string) as placement_id,
            null as post_click_conversions,
            null as post_view_conversions,
            null as posts,
            null as revenue
        from {{ ref("src_ads_creative_facebook_all_data") }}
    )
select *
from pre_proc
