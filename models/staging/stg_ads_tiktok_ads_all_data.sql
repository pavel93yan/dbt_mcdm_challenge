{{ config(materialized="view", tags=["daily"]) }}

with
    pre_proc as (
        select
            cast(__insert_date as datetime) as __insert_date,
            cast(ad_id as string) as ad_id,
            coalesce(cast(add_to_cart as int64), 0) as add_to_cart,
            cast(adgroup_id as string) as adset_id,
            cast(campaign_id as string) as campaign_id,
            cast(channel as string) as channel,
            coalesce(cast(clicks as int64), 0) as clicks,
            coalesce(cast(impressions as int64), 0) as impressions,
            coalesce(cast(spend as int64), 0) as spend,
            cast(nullif(trim(ad_text), '') as string) as ad_text,
            cast(nullif(trim(ad_status), '') as string) as ad_status,
            cast(`date` as date) as `date`,
            coalesce(cast(video_views as int64), 0) as video_views,
            coalesce(cast((rt_installs + skan_app_install) as int64), 0) as installs,
            coalesce(cast(purchase as int64), 0) as purchase,
            coalesce(cast(registrations as int64), 0) as registrations,
            coalesce(
                cast((skan_conversion + conversions) as int64), 0
            ) as total_conversions,

            -- down below fields that are not in data source 
            null as engagements,
            cast(null as string) as placement_id,
            null as post_click_conversions,
            null as post_view_conversions,
            null as posts,
            null as revenue,
            null as comments,
            cast(null as string) as creative_id,
            null as likes,
            null as shares,
            null as link_clicks

        from {{ ref("src_ads_tiktok_ads_all_data") }}
    )
select *
from pre_proc
