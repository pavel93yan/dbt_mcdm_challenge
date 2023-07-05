{{ config(materialized="view", tags=["daily"]) }}

with
    pre_proc as (
        select
            cast(__insert_date as datetime) as __insert_date,
            cast(ad_id as string) as ad_id,
            cast(adset_id as string) as adset_id,
            cast(campaign_id as string) as campaign_id,
            cast(channel as string) as channel,
            cast(nullif(trim(ad_description), '') as string) as ad_description,
            cast(nullif(trim(title_part_1), '') as string) as title_part_1,
            cast(nullif(trim(title_part_2), '') as string) as title_part_2,
            coalesce(cast(clicks as int64), 0) as clicks,
            cast(`date` as date) as `date`,
            coalesce(cast(imps as int64), 0) as impressions,
            coalesce(cast(revenue as int64), 0) as revenue,
            coalesce(cast(spend as int64), 0) as spend,
            coalesce(cast(conv as int64), 0) as total_conversions,
            coalesce(cast(conv as int64), 0) as purchase,

            -- down below fields that are not in data source 
            null as add_to_cart,
            null as comments,
            cast(null as string) as creative_id,
            null as engagements,
            null as installs,
            null as likes,
            null as link_clicks,
            cast(null as string) as placement_id,
            null as post_click_conversions,
            null as post_view_conversions,
            null as posts,
            null as registrations,
            null as shares,
            null as video_views
        from {{ ref("src_ads_bing_all_data") }}
    )
select *
from pre_proc
