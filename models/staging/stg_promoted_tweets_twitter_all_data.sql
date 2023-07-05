{{ config(materialized="view", tags=["daily"]) }}

{{ config(materialized="view", tags=["daily"]) }}

with
    pre_proc as (
        select
            cast(__insert_date as datetime) as __insert_date,
            cast(campaign_id as string) as ad_id,
            cast(campaign_id as string) as adset_id,
            cast(campaign_id as string) as campaign_id,
            cast(channel as string) as channel,
            coalesce(cast(clicks as int64), 0) as clicks,
            coalesce(cast(impressions as int64), 0) as impressions,
            coalesce(cast(spend as int64), 0) as spend,
            cast(nullif(trim(text), '') as string) as text,
            cast(nullif(trim(url), '') as string) as url,
            cast(`date` as date) as `date`,
            coalesce(cast(video_total_views as int64), 0) as video_views,
            coalesce(cast(comments as int64), 0) as comments,
            coalesce(cast(engagements as int64), 0) as engagements,
            coalesce(cast(likes as int64), 0) as likes,
            coalesce(cast(url_clicks as int64), 0) as link_clicks,
            coalesce(cast(retweets as int64), 0) as shares,

            -- down below fields that are not in data source 
            cast(null as string) as placement_id,
            null as post_click_conversions,
            null as post_view_conversions,
            null as posts,
            null as revenue,
            cast(null as string) as creative_id,
            null as add_to_cart,
            null as installs,
            null as total_conversions,
            null as purchase,
            null as registrations
        from {{ ref("src_promoted_tweets_twitter_all_data") }}
    )
select *
from pre_proc
