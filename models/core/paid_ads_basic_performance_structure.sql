{{ config(materialized="table", tags=["daily"]) }}


{%- call statement("build_column_names", fetch_result=True) -%}
    select string_agg(mcdm_field_name, ', ')
    from {{ ref("mcdm_paid_ads_basic_performance_structure") }}
{%- endcall -%}

{%- set var_column_names = load_result("build_column_names")["data"][0][0] -%}


with
    bing as (select * from {{ ref("stg_ads_bing_all_data") }}),
    facebook as (select * from {{ ref("stg_ads_creative_facebook_all_data") }}),
    tiktok as (select * from {{ ref("stg_ads_tiktok_ads_all_data") }}),
    twitter as (select * from {{ ref("stg_promoted_tweets_twitter_all_data") }})

select {{ var_column_names }}
from bing
union all
select {{ var_column_names }}
from facebook
union all
select {{ var_column_names }}
from tiktok
union all
select {{ var_column_names }}
from twitter

-- depends_on: {{ ref('mcdm_paid_ads_basic_performance_structure') }} 